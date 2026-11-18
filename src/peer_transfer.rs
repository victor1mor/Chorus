use crate::peer_wire::{PeerMessage, PeerState};
use crate::quadro::{AparelhoId, EstadoPeca, QuadroDePecas};
use crate::throttle::RateLimiter;
use crate::torrent_storage::{Layout, TorrentStorage};
use anyhow::{Result, anyhow};
use futures::{Stream, StreamExt, future::BoxFuture, stream};
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::future::poll_fn;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, WriteHalf};
use tokio::sync::{Semaphore, mpsc};
use tokio::time::{Instant, sleep_until, timeout};

const BLOCK_SIZE: u32 = 16 * 1024;
const MIN_PENDING_REQUESTS: usize = 5;
const MAX_PENDING_REQUESTS: usize = 250;
const REQUEST_QUEUE_SECS: u64 = 3;
const RATE_TICK: Duration = Duration::from_secs(1);
/// Our local id for `ut_pex` (BEP 11), announced in the extended handshake.
const PEX_ID: u8 = 1;
/// BEP 11 allows 50 per message after the first; the first may carry more.
const MAX_PEX_POR_MENSAGEM: usize = 200;
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const BLOCK_TIMEOUT: Duration = Duration::from_secs(20);
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(90);
const SILENCE_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_INVALID_BLOCKS: usize = 32;

#[derive(Debug)]
pub(crate) struct AparelhoHostil(anyhow::Error);

impl std::fmt::Display for AparelhoHostil {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for AparelhoHostil {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

pub(crate) enum EventoAparelho {
    Conectou,
    Gravou(usize),
    /// Addresses learned from a peer through PEX (BEP 11); untrusted until filtered.
    Descobriu(Vec<std::net::SocketAddr>),
}

pub(crate) struct PeerJob {
    pub quadro: Mutex<QuadroDePecas>,
    pub storage: Arc<TorrentStorage>,
    pub layout: Arc<Layout>,
    pub hashes: Vec<[u8; 20]>,
    pub semaforo: Arc<Semaphore>,
    pub global_limiter: Arc<RateLimiter>,
    pub job_limiter: RateLimiter,
    /// PEX allowed (never for a private torrent).
    pub pex: bool,
}

pub(crate) fn tamanho_peca(layout: &Layout, indice: usize) -> u64 {
    layout
        .piece_length
        .min(layout.total_length - indice as u64 * layout.piece_length)
}

/// SHA-1 check and write of a whole piece under the job semaphore; false = wrong hash, nothing written.
pub(crate) async fn conferir_e_gravar_peca(
    job: &PeerJob,
    peca: usize,
    dados: Vec<u8>,
) -> Result<bool> {
    let esperado = job.hashes[peca];
    let storage = Arc::clone(&job.storage);
    conferir_e_gravar(Arc::clone(&job.semaforo), move || {
        if <[u8; 20]>::from(Sha1::digest(&dados)) != esperado {
            return Ok(false);
        }
        storage.write_piece(peca, &dados)?;
        Ok(true)
    })
    .await
}

async fn conferir_e_gravar<T: Send + 'static>(
    semaforo: Arc<Semaphore>,
    trabalho: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    let licenca = semaforo.acquire_owned().await?;
    tokio::task::spawn_blocking(move || {
        let _licenca = licenca;
        trabalho()
    })
    .await?
}

pub(crate) struct PeerTransfer<'a, S> {
    writer: WriteHalf<&'a mut S>,
    messages: Pin<Box<dyn Stream<Item = io::Result<PeerMessage>> + Send + 'a>>,
    job: &'a PeerJob,
    id: AparelhoId,
    eventos: &'a mpsc::UnboundedSender<EventoAparelho>,
    state: PeerState,
    invalid_blocks: usize,
    last_received: Instant,
    last_sent: Instant,
    amostra: u64,
    media: u64,
    proximo_tique: Instant,
    /// Bandwidth already reserved for the next Request; it may be sent at this instant.
    liberado_em: Option<Instant>,
    credito: bool,
    fast: bool,
    extensao: bool,
    /// BEP 10 `reqq`: the most requests this peer accepts in flight.
    reqq: Option<usize>,
}

struct DownloadingPiece {
    index: u32,
    data: Vec<u8>,
    next_begin: u32,
}

struct BlockRequest {
    index: u32,
    begin: u32,
    length: u32,
    deadline: Instant,
}

struct CheckingPiece {
    index: u32,
    length: u32,
    result: BoxFuture<'static, Result<bool>>,
}

impl<'a, S: AsyncRead + AsyncWrite + Unpin + Send> PeerTransfer<'a, S> {
    pub(crate) fn new(
        stream: &'a mut S,
        job: &'a PeerJob,
        id: AparelhoId,
        eventos: &'a mpsc::UnboundedSender<EventoAparelho>,
    ) -> Self {
        let (reader, writer) = tokio::io::split(stream);
        // Keep partial frames in the read future when a timer wins the select.
        let messages = Box::pin(stream::unfold(reader, |mut reader| async {
            let message = PeerMessage::read_async(&mut reader).await;
            Some((message, reader))
        }));
        let now = Instant::now();
        Self {
            writer,
            messages,
            job,
            id,
            eventos,
            state: PeerState::default(),
            invalid_blocks: 0,
            last_received: now,
            last_sent: now,
            amostra: 0,
            media: 0,
            proximo_tique: now + RATE_TICK,
            liberado_em: None,
            credito: false,
            fast: false,
            extensao: false,
            reqq: None,
        }
    }

    /// Extensions negotiated in the handshake (both sides set the reserved bit).
    pub(crate) fn com_extensoes(mut self, fast: bool, extensao: bool) -> Self {
        self.fast = fast;
        self.extensao = extensao;
        self
    }

    fn fila(&self) -> usize {
        fila_desejada(self.media).min(self.reqq.unwrap_or(MAX_PENDING_REQUESTS))
    }

    pub(crate) async fn download(&mut self) -> Result<()> {
        if self.fast {
            // BEP 6: exactly one of Bitfield, HaveAll or HaveNone right after the handshake.
            // We do not upload, so we offer nothing.
            self.send(PeerMessage::HaveNone).await?;
        }
        if self.extensao {
            // BEP 10: our extended handshake. Empty `m`: we only read the peer's `reqq`.
            let hello: &[u8] = if self.job.pex {
                b"d1:md6:ut_pexi1eee"
            } else {
                b"d1:mdee"
            };
            self.send(PeerMessage::Extended(0, hello.to_vec())).await?;
        }
        self.send(PeerMessage::Interested).await?;
        let mut bad_pieces = 0;
        let mut pieces: Vec<DownloadingPiece> = Vec::new();
        let mut pending: Vec<BlockRequest> = Vec::with_capacity(MIN_PENDING_REQUESTS);
        // ponytail: scan the checks bounded by the board's budget. Index the
        // checks by piece and poll only ready futures if this scan becomes costly.
        let mut checking: Vec<CheckingPiece> = Vec::new();
        // Requests cover a contiguous prefix. Keep only its end for each abandoned piece.
        let mut abandoned = HashMap::<u32, u32>::new();
        let mut disconnected: Option<io::Error> = None;
        'download: loop {
            checking.retain(|piece| {
                let assigned = self.tem_atribuicao(piece.index);
                if !assigned {
                    abandoned.insert(piece.index, piece.length);
                }
                assigned
            });
            if disconnected.is_none() {
                let (quadro, id) = (&self.job.quadro, self.id);
                for piece in pieces.extract_if(.., |p| {
                    !still_assigned(&quadro.lock().unwrap(), id, p.index)
                }) {
                    remember_abandoned(&mut abandoned, piece.index, piece.next_begin);
                    self.cancel_piece(piece.index, &mut pending).await?;
                }
                if let Some(request) = pending.first()
                    && Instant::now() >= request.deadline
                {
                    let index = request.index;
                    let pos = pieces
                        .iter()
                        .position(|piece| piece.index == index)
                        .unwrap();
                    let piece = pieces.remove(pos);
                    remember_abandoned(&mut abandoned, index, piece.next_begin);
                    self.cancel_piece(index, &mut pending).await?;
                    let _ = self
                        .job
                        .quadro
                        .lock()
                        .unwrap()
                        .soltar_por_atraso(self.id, index as usize);
                    continue;
                }
            }

            if checking.is_empty() {
                if pieces.is_empty() && self.job.quadro.lock().unwrap().completo() {
                    return Ok(());
                }
                if let Some(err) = disconnected {
                    return Err(err.into());
                }
            }
            while disconnected.is_none()
                && !self.state.peer_choking
                && pending.len() < self.fila()
                && self.liberado_em.is_none()
            {
                let piece = if let Some(pos) = pieces
                    .iter()
                    .position(|p| (p.next_begin as usize) < p.data.len())
                {
                    &mut pieces[pos]
                } else {
                    let assigned = {
                        let quadro = &mut *self.job.quadro.lock().unwrap();
                        // Don't reacquire an index that was lost during a write.
                        if pieces
                            .iter()
                            .any(|p| !still_assigned(quadro, self.id, p.index))
                            || checking
                                .iter()
                                .any(|p| !still_assigned(quadro, self.id, p.index))
                        {
                            continue 'download;
                        }
                        quadro.pedir(self.id, Instant::now())
                    };
                    let Some(assigned) = assigned else {
                        break;
                    };
                    pieces.push(DownloadingPiece {
                        index: assigned.peca as u32,
                        data: vec![0; tamanho_peca(&self.job.layout, assigned.peca) as usize],
                        next_begin: 0,
                    });
                    pieces.last_mut().unwrap()
                };
                if !self.tem_atribuicao(piece.index) {
                    continue 'download;
                }
                let index = piece.index;
                let begin = piece.next_begin;
                let length = (piece.data.len() as u32 - begin).min(BLOCK_SIZE);
                // ADR 3.6: wait for bandwidth BEFORE asking, without stopping the read loop.
                if !self.credito {
                    let bytes = u64::from(length);
                    let espera = self
                        .job
                        .global_limiter
                        .reservar(bytes)
                        .max(self.job.job_limiter.reservar(bytes));
                    if !espera.is_zero() {
                        self.liberado_em = Some(Instant::now() + espera);
                        break;
                    }
                }
                self.credito = false;
                self.send(PeerMessage::Request {
                    index,
                    begin,
                    length,
                })
                .await?;
                pending.push(BlockRequest {
                    index,
                    begin,
                    length,
                    deadline: Instant::now() + BLOCK_TIMEOUT,
                });
                piece.next_begin += length;
            }

            let connected = disconnected.is_none();
            // Our own bandwidth wait is not the peer's silence.
            let liberado = self.liberado_em;
            let silence = self
                .last_received
                .max(liberado.unwrap_or(self.last_received))
                + SILENCE_TIMEOUT;
            let block_deadline = pending.first().map(|request| request.deadline);
            let has_checks = !checking.is_empty();
            let message = tokio::select! {
                biased;
                () = sleep_until(silence), if connected => {
                    return Err(anyhow!("peer was silent for {} s", SILENCE_TIMEOUT.as_secs()));
                }
                () = sleep_until(block_deadline.unwrap_or(silence)), if connected && block_deadline.is_some() => {
                    continue;
                }
                (pos, hash_ok) = poll_fn(|cx| {
                    for (pos, piece) in checking.iter_mut().enumerate() {
                        if let Poll::Ready(result) = piece.result.as_mut().poll(cx) {
                            return Poll::Ready((pos, result));
                        }
                    }
                    Poll::Pending
                }), if has_checks => {
                    let index = checking.remove(pos).index as usize;
                    let hash_ok = hash_ok?;
                    if self.job.quadro.lock().unwrap().concluir(self.id, index, hash_ok, Instant::now()).is_ok() {
                        if hash_ok {
                            let _ = self.eventos.send(EventoAparelho::Gravou(index));
                        } else {
                            bad_pieces += 1;
                            if bad_pieces == 3 {
                                return Err(AparelhoHostil(anyhow!("peer sent bad pieces")).into());
                            }
                        }
                    }
                    continue;
                }
                () = sleep_until(liberado.unwrap_or(silence)), if connected && liberado.is_some() => {
                    self.liberado_em = None;
                    self.credito = true;
                    self.last_received = self.last_received.max(Instant::now());
                    continue;
                }
                () = sleep_until(self.proximo_tique) => {
                    self.media = atualizar_media(self.media, self.amostra);
                    self.amostra = 0;
                    self.proximo_tique += RATE_TICK;
                    continue;
                }
                () = sleep_until(self.last_sent + KEEP_ALIVE_INTERVAL), if connected => {
                    self.send(PeerMessage::KeepAlive).await?;
                    continue;
                }
                message = self.messages.next(), if connected => message.expect("message stream never ends"),
            };
            let message = match message {
                Ok(message) => message,
                Err(err) if err.kind() == io::ErrorKind::InvalidData => {
                    return Err(AparelhoHostil(err.into()).into());
                }
                Err(err) => {
                    // A peer can close immediately after delivering its last block.
                    // Finish the pieces already received before reporting the socket error.
                    pieces.clear();
                    disconnected = Some(err);
                    continue;
                }
            };
            self.last_received = Instant::now();
            self.apply_message(&message)?;
            match message {
                PeerMessage::Choke => {
                    let mut quadro = self.job.quadro.lock().unwrap();
                    // ponytail: chokes restart incomplete pieces. Keep partial blocks
                    // across chokes if peers with short unchoke windows need support.
                    for piece in pieces.drain(..) {
                        let _ = quadro.soltar(self.id, piece.index as usize);
                        remember_abandoned(&mut abandoned, piece.index, piece.next_begin);
                    }
                    pending.clear();
                }
                PeerMessage::RejectRequest {
                    index,
                    begin,
                    length,
                } => {
                    let pos = pending.iter().position(|request| {
                        request.index == index && request.begin == begin && request.length == length
                    });
                    match pos {
                        Some(pos) => {
                            // BEP 6: the peer will not send this block. Give the piece back.
                            pending.remove(pos);
                            if let Some(piece_pos) = pieces.iter().position(|p| p.index == index) {
                                let piece = pieces.remove(piece_pos);
                                remember_abandoned(&mut abandoned, index, piece.next_begin);
                            }
                            self.cancel_piece(index, &mut pending).await?;
                            let _ = self
                                .job
                                .quadro
                                .lock()
                                .unwrap()
                                .soltar(self.id, index as usize);
                        }
                        // Every request gets one answer, even after Cancel or Choke.
                        None if begin < abandoned.get(&index).copied().unwrap_or(0) => {}
                        None => {
                            return Err(AparelhoHostil(anyhow!(
                                "peer rejected a request never sent"
                            ))
                            .into());
                        }
                    }
                }
                PeerMessage::Piece {
                    index,
                    begin,
                    block,
                } => {
                    let pos = pending.iter().position(|request| {
                        request.index == index
                            && request.begin == begin
                            && request.length as usize == block.len()
                    });
                    if let Some(pos) = pos {
                        pending.remove(pos);
                        self.amostra += block.len() as u64;
                    } else {
                        let lost_now = pieces
                            .iter()
                            .find(|p| p.index == index)
                            .map(|p| p.next_begin)
                            .or_else(|| {
                                checking.iter().find(|p| p.index == index).map(|p| p.length)
                            })
                            .filter(|_| !self.tem_atribuicao(index))
                            .unwrap_or(0);
                        let end = abandoned.get(&index).copied().unwrap_or(0).max(lost_now);
                        let late = begin < end
                            && begin.is_multiple_of(BLOCK_SIZE)
                            && block.len()
                                == (tamanho_peca(&self.job.layout, index as usize)
                                    - u64::from(begin))
                                .min(u64::from(BLOCK_SIZE))
                                    as usize;
                        if !late {
                            self.reject_block()?;
                            continue;
                        }
                    }
                    if pos.is_none() || !self.tem_atribuicao(index) {
                        continue;
                    }
                    let pos = pieces
                        .iter()
                        .position(|piece| piece.index == index)
                        .unwrap();
                    let piece = &mut pieces[pos];
                    let begin = begin as usize;
                    piece.data[begin..begin + block.len()].copy_from_slice(&block);
                    if piece.next_begin as usize == piece.data.len()
                        && !pending.iter().any(|request| request.index == index)
                    {
                        let data = pieces.remove(pos).data;
                        let expected = self.job.hashes[index as usize];
                        let storage = Arc::clone(&self.job.storage);
                        checking.push(CheckingPiece {
                            index,
                            length: data.len() as u32,
                            result: Box::pin(conferir_e_gravar(
                                Arc::clone(&self.job.semaforo),
                                move || {
                                    if <[u8; 20]>::from(Sha1::digest(&data)) != expected {
                                        return Ok(false);
                                    }
                                    storage.write_piece(index as usize, &data)?;
                                    Ok(true)
                                },
                            )),
                        });
                    }
                }
                _ => {}
            }
        }
    }

    async fn send(&mut self, message: PeerMessage) -> Result<()> {
        timeout(WRITE_TIMEOUT, message.write_async(&mut self.writer))
            .await
            .map_err(|_| socket_timeout())??;
        self.last_sent = Instant::now();
        Ok(())
    }

    fn reject_block(&mut self) -> Result<()> {
        self.invalid_blocks += 1;
        if self.invalid_blocks >= MAX_INVALID_BLOCKS {
            return Err(AparelhoHostil(anyhow!("peer sent invalid blocks")).into());
        }
        Ok(())
    }

    fn apply_message(&mut self, message: &PeerMessage) -> Result<()> {
        match message {
            PeerMessage::Bitfield(bits) => self
                .job
                .quadro
                .lock()
                .unwrap()
                .bitfield(self.id, bits)
                .map_err(AparelhoHostil)?,
            PeerMessage::Have(index) => self
                .job
                .quadro
                .lock()
                .unwrap()
                .have(self.id, *index as usize)
                .map_err(AparelhoHostil)?,
            PeerMessage::HaveAll
            | PeerMessage::HaveNone
            | PeerMessage::SuggestPiece(_)
            | PeerMessage::AllowedFast(_)
            | PeerMessage::RejectRequest { .. }
                if !self.fast =>
            {
                // BEP 6: without the fast extension these MUST close the connection.
                return Err(AparelhoHostil(anyhow!("fast message without fast extension")).into());
            }
            PeerMessage::HaveAll => {
                let pecas = self.job.hashes.len();
                let mut bits = vec![0xff; pecas.div_ceil(8)];
                if !pecas.is_multiple_of(8) {
                    *bits.last_mut().unwrap() = 0xff << (8 - pecas % 8);
                }
                self.job
                    .quadro
                    .lock()
                    .unwrap()
                    .bitfield(self.id, &bits)
                    .map_err(AparelhoHostil)?;
            }
            // Advisory (BEP 6): we only download from peers that unchoke us.
            PeerMessage::HaveNone | PeerMessage::SuggestPiece(_) | PeerMessage::AllowedFast(_) => {}
            PeerMessage::Extended(PEX_ID, payload) if self.job.pex => {
                let novos = ler_pex(payload);
                if !novos.is_empty() {
                    let _ = self.eventos.send(EventoAparelho::Descobriu(novos));
                }
            }
            PeerMessage::Extended(0, payload) => {
                self.reqq = ler_reqq(payload).or(self.reqq);
            }
            _ => self.state.apply_message(message),
        }
        Ok(())
    }

    fn tem_atribuicao(&self, index: u32) -> bool {
        still_assigned(&self.job.quadro.lock().unwrap(), self.id, index)
    }

    async fn cancel_piece(&mut self, index: u32, pending: &mut Vec<BlockRequest>) -> Result<()> {
        for request in pending.extract_if(.., |request| request.index == index) {
            self.send(PeerMessage::Cancel {
                index,
                begin: request.begin,
                length: request.length,
            })
            .await?;
        }
        Ok(())
    }
}

/// Addresses from a BEP 11 `ut_pex` message (`added` and `added6`, compact form).
/// Untrusted: the job still filters every address. Malformed messages give nothing.
fn ler_pex(payload: &[u8]) -> Vec<std::net::SocketAddr> {
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
    let limites = crate::bencode::DecodeLimits {
        max_depth: 8,
        max_items: 256,
        max_byte_len: 64 * 1024,
    };
    let Ok(crate::bencode::BValue::Dict(dict)) =
        crate::bencode::decode_with_limits(payload, limites)
    else {
        return Vec::new();
    };
    let mut novos = Vec::new();
    if let Some(crate::bencode::BValue::Bytes(v4)) = dict.get(b"added".as_slice())
        && v4.len() % 6 == 0
    {
        novos.extend(v4.as_chunks::<6>().0.iter().map(|c| {
            let ip = Ipv4Addr::new(c[0], c[1], c[2], c[3]);
            SocketAddr::from((ip, u16::from_be_bytes([c[4], c[5]])))
        }));
    }
    if let Some(crate::bencode::BValue::Bytes(v6)) = dict.get(b"added6".as_slice())
        && v6.len() % 18 == 0
    {
        novos.extend(v6.as_chunks::<18>().0.iter().map(|c| {
            let ip = Ipv6Addr::from(<[u8; 16]>::try_from(&c[..16]).unwrap());
            SocketAddr::from((ip, u16::from_be_bytes([c[16], c[17]])))
        }));
    }
    novos.truncate(MAX_PEX_POR_MENSAGEM);
    novos
}

/// `reqq` from a BEP 10 extended handshake, capped by our own queue limit.
fn ler_reqq(payload: &[u8]) -> Option<usize> {
    let limites = crate::bencode::DecodeLimits {
        max_depth: 8,
        max_items: 256,
        max_byte_len: 64 * 1024,
    };
    let crate::bencode::BValue::Dict(dict) =
        crate::bencode::decode_with_limits(payload, limites).ok()?
    else {
        return None;
    };
    match dict.get(b"reqq".as_slice()) {
        Some(crate::bencode::BValue::Int(n)) if *n >= 1 => {
            Some(usize::try_from(*n).map_or(MAX_PENDING_REQUESTS, |n| n.min(MAX_PENDING_REQUESTS)))
        }
        _ => None,
    }
}

/// One-second sliding average, as libtorrent's `stat_channel::second_tick`.
fn atualizar_media(media: u64, amostra: u64) -> u64 {
    media * 4 / 5 + amostra / 5
}

/// Requests for `REQUEST_QUEUE_SECS` of the measured rate, between the ADR limits.
fn fila_desejada(media: u64) -> usize {
    let blocos = REQUEST_QUEUE_SECS * media / u64::from(BLOCK_SIZE);
    usize::try_from(blocos)
        .unwrap_or(MAX_PENDING_REQUESTS)
        .clamp(MIN_PENDING_REQUESTS, MAX_PENDING_REQUESTS)
}

fn still_assigned(quadro: &QuadroDePecas, id: AparelhoId, index: u32) -> bool {
    matches!(
        quadro.estado(index as usize),
        Some(EstadoPeca::EmVoo { dono, ajudantes, .. })
            if *dono == id || ajudantes.iter().any(|&(ajudante, _)| ajudante == id)
    )
}

fn remember_abandoned(abandoned: &mut HashMap<u32, u32>, index: u32, next_begin: u32) {
    let end = abandoned.entry(index).or_default();
    *end = (*end).max(next_begin);
}

pub(crate) fn socket_timeout() -> io::Error {
    // Match the errors from the old blocking socket read/write timeouts.
    #[cfg(windows)]
    return io::Error::from_raw_os_error(10060); // WSAETIMEDOUT
    #[cfg(target_os = "linux")]
    return io::Error::from_raw_os_error(11); // EAGAIN
    #[cfg(target_os = "macos")]
    return io::Error::from_raw_os_error(35); // EAGAIN
    #[cfg(not(any(windows, target_os = "linux", target_os = "macos")))]
    return io::ErrorKind::TimedOut.into();
}

pub(crate) struct PeerReader<'a> {
    pub stream: &'a mut std::net::TcpStream,
    pub deadline: std::time::Instant,
    pub cancel_flag: Option<&'a std::sync::atomic::AtomicBool>,
}

impl std::io::Read for PeerReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self
            .cancel_flag
            .is_some_and(|flag| flag.load(std::sync::atomic::Ordering::Relaxed))
        {
            return Err(std::io::Error::other("Cancelled"));
        }
        let remaining = self
            .deadline
            .saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return Err(std::io::ErrorKind::TimedOut.into());
        }
        self.stream.set_read_timeout(Some(remaining))?;
        std::io::Read::read(self.stream, buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metainfo::{FileInfo, Info, MultiFileInfo, SingleFileInfo};
    use crate::torrent_storage::TorrentPieceMap;
    use futures::poll;
    use sha1::{Digest, Sha1};
    use std::collections::VecDeque;
    use std::fs;
    use std::io::Write;
    use std::net::{TcpListener, TcpStream as BlockingStream};
    use std::path::{Path, PathBuf};
    use std::pin::pin;
    use std::task::Poll;
    use std::thread;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
    use tokio::net::TcpStream;
    use tokio::time::advance;

    fn f14a_job(dir: &Path, pieces: usize, blocks: u32) -> PeerJob {
        let piece_length = (blocks * BLOCK_SIZE) as usize;
        let hashes = vec![Sha1::digest(vec![0; piece_length]).into(); pieces];
        let info = Info::SingleFile(SingleFileInfo {
            name: "payload.bin".into(),
            length: piece_length * pieces,
            piece_length,
            pieces: hashes.clone(),
        });
        let mut quadro = QuadroDePecas::new(
            vec![piece_length as u64; pieces],
            &TorrentPieceMap::new(pieces),
            (piece_length * pieces) as u64,
            Some(0),
        )
        .unwrap();
        quadro.entrou(1).unwrap();
        PeerJob {
            quadro: Mutex::new(quadro),
            storage: Arc::new(TorrentStorage::new(dir, &info).unwrap()),
            layout: Arc::new(Layout::from_info(&info)),
            hashes,
            semaforo: Arc::new(Semaphore::new(1)),
            global_limiter: Arc::new(RateLimiter::new(0)),
            job_limiter: RateLimiter::new(0),
            pex: false,
        }
    }

    fn f14a_dir(test: &str) -> PathBuf {
        std::env::temp_dir().join(format!("chorus-f14a-{test}-{}", std::process::id()))
    }

    async fn f14a_read(peer: &mut DuplexStream) -> PeerMessage {
        match poll!(pin!(PeerMessage::read_async(peer))) {
            Poll::Ready(message) => message.unwrap(),
            Poll::Pending => panic!("expected a message at the current instant"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn f14a_20s_por_bloco_cancela_pendentes_e_pede_outra_peca() {
        let dir = f14a_dir("blocos");
        let job = f14a_job(&dir, 2, 6);
        let (tx, _) = mpsc::unbounded_channel();
        let (mut client, mut peer) = tokio::io::duplex(128 * 1024);
        {
            let mut transfer = PeerTransfer::new(&mut client, &job, 1, &tx);
            let mut run = pin!(transfer.download());
            assert!(poll!(&mut run).is_pending());
            assert_eq!(f14a_read(&mut peer).await, PeerMessage::Interested);
            PeerMessage::Have(0).write_async(&mut peer).await.unwrap();
            peer.write_all(&[0, 0, 0, 1, 1]).await.unwrap();
            assert!(poll!(&mut run).is_pending());
            for block in 0..5 {
                assert_eq!(
                    f14a_read(&mut peer).await,
                    PeerMessage::Request {
                        index: 0,
                        begin: block * BLOCK_SIZE,
                        length: BLOCK_SIZE
                    }
                );
            }

            advance(Duration::from_secs(19)).await;
            for block in 0..4 {
                PeerMessage::Piece {
                    index: 0,
                    begin: block * BLOCK_SIZE,
                    block: vec![0; BLOCK_SIZE as usize],
                }
                .write_async(&mut peer)
                .await
                .unwrap();
            }
            PeerMessage::Have(1).write_async(&mut peer).await.unwrap();
            assert!(poll!(&mut run).is_pending());
            assert_eq!(
                f14a_read(&mut peer).await,
                PeerMessage::Request {
                    index: 0,
                    begin: 5 * BLOCK_SIZE,
                    length: BLOCK_SIZE
                }
            );
            for block in 0..3 {
                assert_eq!(f14a_read(&mut peer).await, f14b_request(1, block));
            }
            assert!(
                poll!(pin!(peer.read_u8())).is_pending(),
                "Cancel antes de 20 s"
            );
            assert!(matches!(
                job.quadro.lock().unwrap().estado(0),
                Some(EstadoPeca::EmVoo { .. })
            ));

            advance(Duration::from_secs(1)).await;
            assert!(poll!(&mut run).is_pending(), "atraso encerrou a conexão");
            for block in 4..6 {
                assert_eq!(
                    f14a_read(&mut peer).await,
                    PeerMessage::Cancel {
                        index: 0,
                        begin: block * BLOCK_SIZE,
                        length: BLOCK_SIZE
                    }
                );
            }
            assert_eq!(
                job.quadro.lock().unwrap().estado(0),
                Some(&EstadoPeca::NaFila)
            );
            for block in 3..5 {
                assert_eq!(
                    f14a_read(&mut peer).await,
                    PeerMessage::Request {
                        index: 1,
                        begin: block * BLOCK_SIZE,
                        length: BLOCK_SIZE
                    }
                );
            }
            println!(
                "19 s: nenhum Cancel; 20 s: dois Cancel e pedido de outra peça na mesma conexão"
            );
        }
        drop(job);
        fs::remove_file(dir.join("payload.bin")).unwrap();
        fs::remove_dir(dir).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn f14a_120s_sem_mensagem_e_keepalive_recebido_renova_sem_banir() {
        let dir = f14a_dir("silencio");
        let job = f14a_job(&dir, 1, 1);
        let (tx, _) = mpsc::unbounded_channel();
        for keepalive in [false, true] {
            let (mut client, mut peer) = tokio::io::duplex(64);
            let mut transfer = PeerTransfer::new(&mut client, &job, 1, &tx);
            let mut run = pin!(transfer.download());
            assert!(poll!(&mut run).is_pending());
            assert_eq!(f14a_read(&mut peer).await, PeerMessage::Interested);
            if keepalive {
                advance(Duration::from_secs(100)).await;
                PeerMessage::KeepAlive.write_async(&mut peer).await.unwrap();
                assert!(
                    poll!(&mut run).is_pending(),
                    "caiu antes do KeepAlive aos 100 s"
                );
            }
            advance(Duration::from_secs(119)).await;
            assert!(
                poll!(&mut run).is_pending(),
                "caiu antes de 120 s de silêncio"
            );
            advance(Duration::from_secs(1)).await;
            let Poll::Ready(result) = poll!(&mut run) else {
                panic!("ainda conectado após 120 s de silêncio");
            };
            let err = result.unwrap_err();
            assert_eq!(err.to_string(), "peer was silent for 120 s");
            assert!(!err.is::<AparelhoHostil>());
            println!(
                "silêncio sem banir aos {} s",
                if keepalive { 220 } else { 120 }
            );
        }
        drop(job);
        fs::remove_file(dir.join("payload.bin")).unwrap();
        fs::remove_dir(dir).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn f14a_keepalive_de_quatro_bytes_aos_90s_enquanto_espera_unchoke() {
        let dir = f14a_dir("keepalive");
        let job = f14a_job(&dir, 1, 1);
        let (tx, _) = mpsc::unbounded_channel();
        let (mut client, mut peer) = tokio::io::duplex(64);
        {
            let mut transfer = PeerTransfer::new(&mut client, &job, 1, &tx);
            let mut run = pin!(transfer.download());
            assert!(poll!(&mut run).is_pending());
            assert_eq!(f14a_read(&mut peer).await, PeerMessage::Interested);
            advance(Duration::from_secs(89)).await;
            // Start a frame before the deadline and finish it after our KeepAlive.
            peer.write_all(&[0, 0]).await.unwrap();
            assert!(poll!(&mut run).is_pending());
            assert!(poll!(pin!(peer.read_u8())).is_pending());
            advance(Duration::from_secs(1)).await;
            assert!(poll!(&mut run).is_pending());
            let mut wire = [0xff; 4];
            assert!(matches!(
                poll!(pin!(peer.read_exact(&mut wire))),
                Poll::Ready(Ok(4))
            ));
            assert_eq!(wire, [0; 4]);
            advance(Duration::from_secs(10)).await;
            peer.write_all(&[0, 0]).await.unwrap();
            PeerMessage::Have(0).write_async(&mut peer).await.unwrap();
            peer.write_all(&[0, 0, 0, 1, 1]).await.unwrap();
            assert!(poll!(&mut run).is_pending());
            assert_eq!(
                f14a_read(&mut peer).await,
                PeerMessage::Request {
                    index: 0,
                    begin: 0,
                    length: BLOCK_SIZE
                }
            );
            println!("89 s: nenhum byte; 90 s: [0, 0, 0, 0]; cabeçalho recebido em duas partes");
        }
        drop(job);
        fs::remove_file(dir.join("payload.bin")).unwrap();
        fs::remove_dir(dir).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn f14b3_limitador_antes_do_pedido_sem_expirar_nem_silencio() {
        // F1.4b.3 replaces F1.4a rule 9: the wait happens BEFORE each Request.
        const BLOCKS: u32 = 4;
        for seconds_per_block in [32, 128] {
            let dir = f14a_dir("limitador");
            let mut job = f14a_job(&dir, 1, BLOCKS);
            job.job_limiter = RateLimiter::new(u64::from(BLOCK_SIZE) / seconds_per_block);
            let (tx, mut rx) = mpsc::unbounded_channel();
            let (mut client, mut peer) = tokio::io::duplex(128 * 1024);
            let start = Instant::now();
            let mut transfer = PeerTransfer::new(&mut client, &job, 1, &tx);
            // If the transfer drops the peer, the fake would wait forever: bound it.
            let limite = Duration::from_secs(seconds_per_block * u64::from(BLOCKS) + 60);
            let (result, fake) = tokio::join!(
                transfer.download(),
                timeout(limite, async {
                    assert_eq!(
                        PeerMessage::read_async(&mut peer).await.unwrap(),
                        PeerMessage::Interested
                    );
                    PeerMessage::Have(0).write_async(&mut peer).await.unwrap();
                    peer.write_all(&[0, 0, 0, 1, 1]).await.unwrap();
                    for block in 0..BLOCKS {
                        let begin = block * BLOCK_SIZE;
                        assert_eq!(
                            loop {
                                // A KeepAlive at 90 s while we wait for bandwidth is correct.
                                match PeerMessage::read_async(&mut peer).await.unwrap() {
                                    PeerMessage::KeepAlive => continue,
                                    other => break other,
                                }
                            },
                            PeerMessage::Request {
                                index: 0,
                                begin,
                                length: BLOCK_SIZE
                            }
                        );
                        // Each Request leaves only after its bandwidth: block k at (k + 1) * s.
                        let esperado =
                            Duration::from_secs(seconds_per_block * (u64::from(block) + 1));
                        let saiu = start.elapsed();
                        assert!(
                            saiu >= esperado && saiu < esperado + Duration::from_secs(1),
                            "Request {block} saiu em {saiu:?}, esperado {esperado:?}"
                        );
                        PeerMessage::Piece {
                            index: 0,
                            begin,
                            block: vec![0; BLOCK_SIZE as usize],
                        }
                        .write_async(&mut peer)
                        .await
                        .unwrap();
                    }
                })
            );
            fake.unwrap_or_else(|_| panic!("aparelho caiu durante a espera: {result:?}"));
            while let Poll::Ready(message) = poll!(pin!(PeerMessage::read_async(&mut peer))) {
                assert_eq!(
                    message.unwrap(),
                    PeerMessage::KeepAlive,
                    "houve Cancel ou novo Request"
                );
            }
            result.unwrap();
            assert!(matches!(rx.try_recv(), Ok(EventoAparelho::Gravou(0))));
            assert!(job.quadro.lock().unwrap().completo());
            assert!(start.elapsed() >= Duration::from_secs(seconds_per_block * u64::from(BLOCKS)));
            println!(
                "limitador antes do pedido: {} s, 4 Request espaçados, peça gravada, nenhum Cancel nem silêncio",
                start.elapsed().as_secs()
            );
            drop(transfer);
            drop(job);
            fs::remove_file(dir.join("payload.bin")).unwrap();
            fs::remove_dir(dir).unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn f14a1_fonte_unica_grava_a_peca_atrasada_depois_da_outra() {
        let dir = f14a_dir("ultimo-recurso");
        let job = f14a_job(&dir, 2, 1);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let (mut client, mut peer) = tokio::io::duplex(128 * 1024);
        let mut transfer = PeerTransfer::new(&mut client, &job, 1, &tx);
        let result = {
            let mut run = pin!(transfer.download());
            assert!(poll!(&mut run).is_pending());
            assert_eq!(f14a_read(&mut peer).await, PeerMessage::Interested);
            PeerMessage::Have(0).write_async(&mut peer).await.unwrap();
            peer.write_all(&[0, 0, 0, 1, 1]).await.unwrap();
            assert!(poll!(&mut run).is_pending());
            assert_eq!(
                f14a_read(&mut peer).await,
                PeerMessage::Request {
                    index: 0,
                    begin: 0,
                    length: BLOCK_SIZE,
                }
            );
            PeerMessage::Have(1).write_async(&mut peer).await.unwrap();
            assert!(poll!(&mut run).is_pending());
            assert_eq!(f14a_read(&mut peer).await, f14b_request(1, 0));
            f14b_block(&mut peer, 1, 0).await;
            assert!(poll!(&mut run).is_pending());
            advance(Duration::from_secs(21)).await;
            assert!(poll!(&mut run).is_pending());
            assert_eq!(
                f14a_read(&mut peer).await,
                PeerMessage::Cancel {
                    index: 0,
                    begin: 0,
                    length: BLOCK_SIZE,
                }
            );
            let (result, ()) = tokio::join!(&mut run, async {
                let request = timeout(Duration::from_secs(10), PeerMessage::read_async(&mut peer))
                    .await
                    .unwrap_or_else(|_| panic!("a peça 0 não foi pedida na mesma conexão"))
                    .unwrap();
                assert_eq!(request, f14b_request(0, 0));
                f14b_block(&mut peer, 0, 0).await;
            });
            result
        };
        result.unwrap();
        assert!(matches!(rx.try_recv(), Ok(EventoAparelho::Gravou(1))));
        assert!(matches!(rx.try_recv(), Ok(EventoAparelho::Gravou(0))));
        assert!(job.quadro.lock().unwrap().completo());
        drop(transfer);
        drop(job);
        let payload = dir.join("payload.bin");
        assert_eq!(
            fs::read(&payload).unwrap(),
            vec![0; 2 * BLOCK_SIZE as usize]
        );
        fs::remove_file(payload).unwrap();
        fs::remove_dir(dir).unwrap();
    }

    fn f14b_request(index: u32, block: u32) -> PeerMessage {
        PeerMessage::Request {
            index,
            begin: block * BLOCK_SIZE,
            length: BLOCK_SIZE,
        }
    }

    async fn f14b_block(peer: &mut DuplexStream, index: u32, block: u32) {
        PeerMessage::Piece {
            index,
            begin: block * BLOCK_SIZE,
            block: vec![0; BLOCK_SIZE as usize],
        }
        .write_async(peer)
        .await
        .unwrap();
    }

    async fn f14b_unchoke(peer: &mut DuplexStream) {
        peer.write_all(&[0, 0, 0, 1, 1]).await.unwrap();
    }

    fn f14b_assert_saved(
        job: PeerJob,
        dir: &Path,
        mut rx: mpsc::UnboundedReceiver<EventoAparelho>,
        written: &[usize],
    ) {
        assert!(job.quadro.lock().unwrap().completo());
        let mut saved = Vec::new();
        while let Ok(EventoAparelho::Gravou(index)) = rx.try_recv() {
            saved.push(index);
        }
        saved.sort_unstable();
        assert_eq!(saved, written);
        let payload = dir.join("payload.bin");
        assert_eq!(
            fs::read(&payload).unwrap(),
            vec![0; job.layout.total_length as usize]
        );
        drop(job);
        fs::remove_file(payload).unwrap();
        fs::remove_dir(dir).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn f14b1_a_cinco_pedidos_em_tres_pecas() {
        let dir = f14a_dir("f14b1-a");
        let job = f14a_job(&dir, 3, 2);
        let (tx, _) = mpsc::unbounded_channel();
        let (mut client, mut peer) = tokio::io::duplex(1024);
        {
            let mut transfer = PeerTransfer::new(&mut client, &job, 1, &tx);
            let mut run = pin!(transfer.download());
            PeerMessage::Bitfield(vec![0xe0])
                .write_async(&mut peer)
                .await
                .unwrap();
            f14b_unchoke(&mut peer).await;
            assert!(poll!(&mut run).is_pending());
            assert_eq!(f14a_read(&mut peer).await, PeerMessage::Interested);
            let mut indexes = Vec::new();
            for count in [2, 2, 1] {
                let PeerMessage::Request {
                    index,
                    begin: 0,
                    length: BLOCK_SIZE,
                } = f14a_read(&mut peer).await
                else {
                    panic!("esperava o primeiro bloco de outra peça");
                };
                assert!(!indexes.contains(&index), "peça repetida: {index}");
                indexes.push(index);
                if count == 2 {
                    assert_eq!(f14a_read(&mut peer).await, f14b_request(index, 1));
                }
            }
            assert!(
                poll!(pin!(peer.read_u8())).is_pending(),
                "sexto pedido em voo"
            );
            println!("5 Request em 3 peças: 2 + 2 + 1; nenhum sexto");
        }
        drop(job);
        fs::remove_file(dir.join("payload.bin")).unwrap();
        fs::remove_dir(dir).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn f14b1_b_le_enquanto_o_semaforo_esta_ocupado() {
        let dir = f14a_dir("f14b1-b");
        let job = f14a_job(&dir, 2, 2);
        let permits = job
            .semaforo
            .try_acquire_many(job.semaforo.available_permits() as u32)
            .unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        let (mut client, mut peer) = tokio::io::duplex(128 * 1024);
        {
            let mut transfer = PeerTransfer::new(&mut client, &job, 1, &tx);
            let mut run = pin!(transfer.download());
            PeerMessage::Have(0).write_async(&mut peer).await.unwrap();
            f14b_unchoke(&mut peer).await;
            assert!(poll!(&mut run).is_pending());
            assert_eq!(f14a_read(&mut peer).await, PeerMessage::Interested);
            for block in 0..2 {
                assert_eq!(f14a_read(&mut peer).await, f14b_request(0, block));
                f14b_block(&mut peer, 0, block).await;
            }
            assert!(poll!(&mut run).is_pending());
            PeerMessage::Have(1).write_async(&mut peer).await.unwrap();
            assert!(poll!(&mut run).is_pending());
            for block in 0..2 {
                assert_eq!(f14a_read(&mut peer).await, f14b_request(1, block));
                f14b_block(&mut peer, 1, block).await;
            }
            // A Choke would release B if it were still incomplete.
            peer.write_all(&[0, 0, 0, 1, 0]).await.unwrap();
            assert!(poll!(&mut run).is_pending());
            for index in 0..2 {
                assert!(matches!(
                    job.quadro.lock().unwrap().estado(index),
                    Some(EstadoPeca::EmVoo { .. })
                ));
            }
            assert!(rx.is_empty());
            drop(permits);
            run.await.unwrap();
        }
        f14b_assert_saved(job, &dir, rx, &[0, 1]);
        println!("A e B recebidas com todas as licenças ocupadas; 2 peças gravadas após liberação");
    }

    #[tokio::test(start_paused = true)]
    async fn f14b1_c_prazo_cancela_so_a_peca_atrasada() {
        let dir = f14a_dir("f14b1-c");
        let job = f14a_job(&dir, 2, 2);
        let (tx, rx) = mpsc::unbounded_channel();
        let (mut client, mut peer) = tokio::io::duplex(128 * 1024);
        {
            let mut transfer = PeerTransfer::new(&mut client, &job, 1, &tx);
            let mut run = pin!(transfer.download());
            PeerMessage::Have(0).write_async(&mut peer).await.unwrap();
            f14b_unchoke(&mut peer).await;
            assert!(poll!(&mut run).is_pending());
            assert_eq!(f14a_read(&mut peer).await, PeerMessage::Interested);
            for block in 0..2 {
                assert_eq!(f14a_read(&mut peer).await, f14b_request(0, block));
            }
            f14b_block(&mut peer, 0, 0).await;
            {
                let mut quadro = job.quadro.lock().unwrap();
                quadro.entrou(2).unwrap();
                quadro.have(2, 0).unwrap();
            }
            advance(Duration::from_secs(1)).await;
            PeerMessage::Have(1).write_async(&mut peer).await.unwrap();
            assert!(poll!(&mut run).is_pending());
            for block in 0..2 {
                assert_eq!(f14a_read(&mut peer).await, f14b_request(1, block));
            }
            f14b_block(&mut peer, 1, 0).await;
            assert!(poll!(&mut run).is_pending());
            advance(Duration::from_secs(18)).await;
            assert!(poll!(&mut run).is_pending());
            assert!(
                poll!(pin!(peer.read_u8())).is_pending(),
                "Cancel antes de 20 s"
            );
            assert!(matches!(
                job.quadro.lock().unwrap().estado(0),
                Some(EstadoPeca::EmVoo { .. })
            ));

            advance(Duration::from_secs(1)).await;
            f14b_block(&mut peer, 1, 1).await;
            assert!(poll!(&mut run).is_pending());
            assert_eq!(
                f14a_read(&mut peer).await,
                PeerMessage::Cancel {
                    index: 0,
                    begin: BLOCK_SIZE,
                    length: BLOCK_SIZE
                }
            );
            assert!(
                poll!(pin!(peer.read_u8())).is_pending(),
                "só A deve ser cancelada"
            );
            {
                let mut quadro = job.quadro.lock().unwrap();
                assert_eq!(quadro.estado(0), Some(&EstadoPeca::NaFila));
                assert_ne!(quadro.estado(1), Some(&EstadoPeca::NaFila));
                assert_eq!(quadro.pedir(2, Instant::now()).unwrap().peca, 0);
                quadro.concluir(2, 0, true, Instant::now()).unwrap();
            }
            run.await.unwrap();
        }
        f14b_assert_saved(job, &dir, rx, &[1]);
        println!("19 s: nenhum Cancel; 20 s: 1 Cancel de A, A NaFila e B gravada na mesma conexão");
    }

    #[tokio::test(start_paused = true)]
    async fn f14b1_d_choke_solta_duas_pecas_incompletas() {
        let dir = f14a_dir("f14b1-d");
        let job = f14a_job(&dir, 2, 2);
        let (tx, rx) = mpsc::unbounded_channel();
        let (mut client, mut peer) = tokio::io::duplex(128 * 1024);
        {
            let mut transfer = PeerTransfer::new(&mut client, &job, 1, &tx);
            let mut run = pin!(transfer.download());
            assert!(poll!(&mut run).is_pending());
            assert_eq!(f14a_read(&mut peer).await, PeerMessage::Interested);
            f14b_unchoke(&mut peer).await;
            for index in 0..2 {
                PeerMessage::Have(index)
                    .write_async(&mut peer)
                    .await
                    .unwrap();
                assert!(poll!(&mut run).is_pending());
                for block in 0..2 {
                    assert_eq!(f14a_read(&mut peer).await, f14b_request(index, block));
                }
                f14b_block(&mut peer, index, 0).await;
            }
            peer.write_all(&[0, 0, 0, 1, 0]).await.unwrap();
            assert!(poll!(&mut run).is_pending());
            for index in 0..2 {
                assert_eq!(
                    job.quadro.lock().unwrap().estado(index),
                    Some(&EstadoPeca::NaFila)
                );
            }
            assert!(
                poll!(pin!(peer.read_u8())).is_pending(),
                "Choke não deve enviar Cancel"
            );
            f14b_unchoke(&mut peer).await;
            assert!(poll!(&mut run).is_pending());
            let mut seen = Vec::new();
            for _ in 0..4 {
                let PeerMessage::Request {
                    index,
                    begin,
                    length: BLOCK_SIZE,
                } = f14a_read(&mut peer).await
                else {
                    panic!("esperava Request de peça reiniciada");
                };
                seen.push((index, begin));
                f14b_block(&mut peer, index, begin / BLOCK_SIZE).await;
            }
            seen.sort_unstable();
            assert_eq!(seen, [(0, 0), (0, BLOCK_SIZE), (1, 0), (1, BLOCK_SIZE)]);
            run.await.unwrap();
        }
        f14b_assert_saved(job, &dir, rx, &[0, 1]);
        println!("Choke: 2 peças NaFila, zero Cancel; Unchoke: 4 Request, 2 peças gravadas");
    }

    #[tokio::test(start_paused = true)]
    async fn f14b1_e_blocos_atrasados_da_peca_perdida_nao_banem() {
        let dir = f14a_dir("f14b1-e");
        let job = f14a_job(&dir, 2, 2);
        let (tx, rx) = mpsc::unbounded_channel();
        let (mut client, mut peer) = tokio::io::duplex(1024 * 1024);
        {
            let mut transfer = PeerTransfer::new(&mut client, &job, 1, &tx);
            let mut run = pin!(transfer.download());
            assert!(poll!(&mut run).is_pending());
            assert_eq!(f14a_read(&mut peer).await, PeerMessage::Interested);
            f14b_unchoke(&mut peer).await;
            for index in 0..2 {
                PeerMessage::Have(index)
                    .write_async(&mut peer)
                    .await
                    .unwrap();
                assert!(poll!(&mut run).is_pending());
                for block in 0..2 {
                    assert_eq!(f14a_read(&mut peer).await, f14b_request(index, block));
                }
            }
            {
                // The board accounts for a second peer's copy, including its memory.
                let mut quadro = job.quadro.lock().unwrap();
                quadro.soltar(1, 0).unwrap();
                quadro.entrou(2).unwrap();
                quadro.have(2, 0).unwrap();
                assert_eq!(quadro.pedir(2, Instant::now()).unwrap().peca, 0);
                quadro.concluir(2, 0, true, Instant::now()).unwrap();
            }
            PeerMessage::KeepAlive.write_async(&mut peer).await.unwrap();
            assert!(poll!(&mut run).is_pending());
            for block in 0..2 {
                assert_eq!(
                    f14a_read(&mut peer).await,
                    PeerMessage::Cancel {
                        index: 0,
                        begin: block * BLOCK_SIZE,
                        length: BLOCK_SIZE
                    }
                );
            }
            for i in 0..40 {
                f14b_block(&mut peer, 0, i % 2).await;
                assert!(
                    poll!(&mut run).is_pending(),
                    "conexão caiu com {} blocos atrasados",
                    i + 1
                );
            }
            for block in 0..2 {
                f14b_block(&mut peer, 1, block).await;
            }
            run.await.unwrap();
        }
        f14b_assert_saved(job, &dir, rx, &[1]);
        println!("2 Cancel de A, 40 blocos atrasados de A, B gravada na mesma conexão");
    }

    #[tokio::test(start_paused = true)]
    async fn f14b1_e_bane_no_32o_bloco_invalido() {
        let dir = f14a_dir("f14b1-invalid");
        let job = f14a_job(&dir, 2, 6);
        let (tx, _) = mpsc::unbounded_channel();
        let (mut client, mut peer) = tokio::io::duplex(128 * 1024);
        {
            let mut transfer = PeerTransfer::new(&mut client, &job, 1, &tx);
            let mut run = pin!(transfer.download());
            PeerMessage::Have(0).write_async(&mut peer).await.unwrap();
            f14b_unchoke(&mut peer).await;
            assert!(poll!(&mut run).is_pending());
            assert_eq!(f14a_read(&mut peer).await, PeerMessage::Interested);
            for block in 0..5 {
                assert_eq!(f14a_read(&mut peer).await, f14b_request(0, block));
            }
            {
                let mut quadro = job.quadro.lock().unwrap();
                quadro.concluir(1, 0, true, Instant::now()).unwrap();
            }
            for _ in 0..31 {
                PeerMessage::Piece {
                    index: 0,
                    begin: 0,
                    block: vec![0; BLOCK_SIZE as usize - 1],
                }
                .write_async(&mut peer)
                .await
                .unwrap();
                assert!(poll!(&mut run).is_pending());
            }
            f14b_block(&mut peer, 0, 5).await; // first block that was never requested
            let Poll::Ready(Err(err)) = poll!(&mut run) else {
                panic!("32 blocos inválidos devem expulsar o aparelho");
            };
            assert!(err.is::<AparelhoHostil>());
        }
        drop(job);
        fs::remove_file(dir.join("payload.bin")).unwrap();
        fs::remove_dir(dir).unwrap();
        println!("31 blocos pedidos de tamanho errado + 1 nunca pedido: aparelho hostil");
    }

    #[test]
    fn f14b2_a_formula_da_fila() {
        let vinte = 20 * u64::from(BLOCK_SIZE);
        let mut media = 0;
        let mut medias = Vec::new();
        let mut filas = Vec::new();
        for _ in 0..6 {
            media = atualizar_media(media, vinte);
            medias.push(media);
            filas.push(fila_desejada(media));
        }
        assert_eq!(
            medias,
            [65_536, 117_964, 159_907, 193_461, 220_304, 241_779]
        );
        assert_eq!(filas, [12, 21, 29, 35, 40, 44]);
        let duzentos = 200 * u64::from(BLOCK_SIZE);
        let mut media = 0;
        let filas: Vec<_> = (0..4)
            .map(|_| {
                media = atualizar_media(media, duzentos);
                fila_desejada(media)
            })
            .collect();
        assert_eq!(filas, [120, 216, 250, 250]);
        assert_eq!(fila_desejada(0), 5);
        assert_eq!(fila_desejada(16_384), 5);
        assert_eq!(fila_desejada(u64::MAX / 4), 250);
    }

    /// Reads every Request available now; any other message fails the test.
    async fn f14b2_ler_pedidos(peer: &mut DuplexStream, voo: &mut VecDeque<(u32, u32)>) {
        loop {
            // unconstrained: the coop budget must not cut a frame between length and body.
            match poll!(pin!(tokio::task::unconstrained(PeerMessage::read_async(
                &mut *peer
            )))) {
                Poll::Ready(Ok(PeerMessage::Request {
                    index,
                    begin,
                    length: BLOCK_SIZE,
                })) => voo.push_back((index, begin)),
                Poll::Ready(other) => panic!("esperava só Request, veio {other:?}"),
                Poll::Pending => return,
            }
        }
    }

    /// Per simulated second: answer `por_segundo` oldest requests, then tick.
    /// Returns the requests in flight right after each tick.
    async fn f14b2_rodar(
        run: &mut Pin<&mut impl Future<Output = Result<()>>>,
        peer: &mut DuplexStream,
        voo: &mut VecDeque<(u32, u32)>,
        segundos: &[usize],
    ) -> Vec<usize> {
        let mut depois = Vec::new();
        for &por_segundo in segundos {
            for _ in 0..por_segundo {
                assert!(poll!(tokio::task::unconstrained(run.as_mut())).is_pending());
                f14b2_ler_pedidos(peer, voo).await;
                let (index, begin) = voo.pop_front().expect("pedido para responder");
                f14b_block(peer, index, begin / BLOCK_SIZE).await;
            }
            assert!(poll!(tokio::task::unconstrained(run.as_mut())).is_pending());
            f14b2_ler_pedidos(peer, voo).await;
            advance(RATE_TICK).await;
            assert!(poll!(tokio::task::unconstrained(run.as_mut())).is_pending());
            f14b2_ler_pedidos(peer, voo).await;
            depois.push(voo.len());
        }
        depois
    }

    #[tokio::test(start_paused = true)]
    async fn f14b2_b_d_fila_cresce_e_encolhe_sem_cancel() {
        let dir = f14a_dir("f14b2-bd");
        let job = f14a_job(&dir, 64, 16);
        let (tx, _) = mpsc::unbounded_channel();
        let (mut client, mut peer) = tokio::io::duplex(4 * 1024 * 1024);
        {
            let mut transfer = PeerTransfer::new(&mut client, &job, 1, &tx);
            let mut run = pin!(transfer.download());
            PeerMessage::Bitfield(vec![0xff; 8])
                .write_async(&mut peer)
                .await
                .unwrap();
            f14b_unchoke(&mut peer).await;
            assert!(poll!(&mut run).is_pending());
            assert_eq!(f14a_read(&mut peer).await, PeerMessage::Interested);
            let mut voo = VecDeque::new();
            f14b2_ler_pedidos(&mut peer, &mut voo).await;
            assert_eq!(voo.len(), 5, "antes do primeiro tique");
            let cresce = f14b2_rodar(&mut run, &mut peer, &mut voo, &[20; 6]).await;
            assert_eq!(cresce, [12, 21, 29, 35, 40, 44]);
            // Rates fall to 2 blocks/s: queue 36, 30, 25. No new Request, no Cancel.
            let encolhe = f14b2_rodar(&mut run, &mut peer, &mut voo, &[2; 3]).await;
            assert_eq!(encolhe, [44, 42, 40]);
            println!("fila: 5 -> {cresce:?}; encolhendo sem Cancel: {encolhe:?}");
        }
        drop(job);
        let _ = fs::remove_file(dir.join("payload.bin"));
        let _ = fs::remove_dir(dir);
    }

    #[tokio::test(start_paused = true)]
    async fn f14b2_c_teto_de_250() {
        let dir = f14a_dir("f14b2-c");
        let job = f14a_job(&dir, 128, 16);
        let (tx, _) = mpsc::unbounded_channel();
        let (mut client, mut peer) = tokio::io::duplex(8 * 1024 * 1024);
        {
            let mut transfer = PeerTransfer::new(&mut client, &job, 1, &tx);
            let mut run = pin!(transfer.download());
            PeerMessage::Bitfield(vec![0xff; 16])
                .write_async(&mut peer)
                .await
                .unwrap();
            f14b_unchoke(&mut peer).await;
            assert!(poll!(&mut run).is_pending());
            assert_eq!(f14a_read(&mut peer).await, PeerMessage::Interested);
            let mut voo = VecDeque::new();
            let filas = f14b2_rodar(&mut run, &mut peer, &mut voo, &[200; 4]).await;
            assert_eq!(filas, [120, 216, 250, 250]);
            println!("200 blocos/s: {filas:?}, nunca 251");
        }
        drop(job);
        let _ = fs::remove_file(dir.join("payload.bin"));
        let _ = fs::remove_dir(dir);
    }

    #[tokio::test(start_paused = true)]
    async fn f14b2_e_bloco_atrasado_nao_sobe_a_fila() {
        let dir = f14a_dir("f14b2-e");
        let job = f14a_job(&dir, 20, 2);
        let (tx, _) = mpsc::unbounded_channel();
        let (mut client, mut peer) = tokio::io::duplex(4 * 1024 * 1024);
        {
            let mut transfer = PeerTransfer::new(&mut client, &job, 1, &tx);
            let mut run = pin!(transfer.download());
            PeerMessage::Bitfield(vec![0xff, 0xff, 0xf0])
                .write_async(&mut peer)
                .await
                .unwrap();
            f14b_unchoke(&mut peer).await;
            assert!(poll!(&mut run).is_pending());
            assert_eq!(f14a_read(&mut peer).await, PeerMessage::Interested);
            let mut voo = VecDeque::new();
            f14b2_ler_pedidos(&mut peer, &mut voo).await;
            assert_eq!(voo.len(), 5);
            let perdida = voo[0].0;
            {
                let mut quadro = job.quadro.lock().unwrap();
                quadro.soltar(1, perdida as usize).unwrap();
                quadro.entrou(2).unwrap();
                quadro.have(2, perdida as usize).unwrap();
                assert_eq!(
                    quadro.pedir(2, Instant::now()).unwrap().peca,
                    perdida as usize
                );
                quadro
                    .concluir(2, perdida as usize, true, Instant::now())
                    .unwrap();
            }
            PeerMessage::KeepAlive.write_async(&mut peer).await.unwrap();
            assert!(poll!(&mut run).is_pending());
            let mut cancelados = 0;
            loop {
                match poll!(pin!(PeerMessage::read_async(&mut peer))) {
                    Poll::Ready(Ok(PeerMessage::Cancel { index, .. })) => {
                        assert_eq!(index, perdida);
                        cancelados += 1;
                    }
                    Poll::Ready(Ok(PeerMessage::Request { index, begin, .. })) => {
                        voo.push_back((index, begin));
                    }
                    Poll::Ready(other) => panic!("mensagem inesperada {other:?}"),
                    Poll::Pending => break,
                }
            }
            assert_eq!(cancelados, 2);
            voo.retain(|&(index, _)| index != perdida);
            assert_eq!(voo.len(), 5);
            for i in 0..40 {
                f14b_block(&mut peer, perdida, i % 2).await;
                assert!(poll!(&mut run).is_pending());
            }
            advance(RATE_TICK).await;
            assert!(poll!(&mut run).is_pending());
            f14b2_ler_pedidos(&mut peer, &mut voo).await;
            assert_eq!(voo.len(), 5, "bloco atrasado subiu a fila");
            println!("2 Cancel, 40 blocos atrasados, fila continua 5");
        }
        drop(job);
        let _ = fs::remove_file(dir.join("payload.bin"));
        let _ = fs::remove_dir(dir);
    }

    #[tokio::test]
    async fn f15_a_mensagens_fast_e_bit_do_handshake_ida_e_volta() {
        let mensagens = [
            PeerMessage::SuggestPiece(7),
            PeerMessage::HaveAll,
            PeerMessage::HaveNone,
            PeerMessage::RejectRequest {
                index: 1,
                begin: 16_384,
                length: 16_384,
            },
            PeerMessage::AllowedFast(3),
        ];
        let (mut a, mut b) = tokio::io::duplex(4096);
        for mensagem in &mensagens {
            mensagem.write_async(&mut a).await.unwrap();
            assert_eq!(&PeerMessage::read_async(&mut b).await.unwrap(), mensagem);
        }
        let hs = crate::peer_wire::Handshake {
            info_hash: [1; 20],
            peer_id: [2; 20],
            supports_extension: true,
            supports_fast: true,
        };
        hs.write_async(&mut a).await.unwrap();
        let lido = crate::peer_wire::Handshake::read_async(&mut b, &[1; 20], &[3; 20])
            .await
            .unwrap();
        assert!(lido.supports_fast && lido.supports_extension);
        println!("5 mensagens BEP 6 ida e volta; bit 0x04 do byte 7 reservado lido");
    }

    async fn f15_iniciar(nome: &str) -> (PathBuf, PeerJob, mpsc::UnboundedSender<EventoAparelho>) {
        let dir = f14a_dir(nome);
        let job = f14a_job(&dir, 2, 2);
        let (tx, _) = mpsc::unbounded_channel();
        (dir, job, tx)
    }

    #[tokio::test(start_paused = true)]
    async fn f15_b_c_e_have_none_primeiro_have_all_e_reqq() {
        let (dir, job, tx) = f15_iniciar("f15-bce").await;
        let (mut client, mut peer) = tokio::io::duplex(128 * 1024);
        {
            let mut transfer =
                PeerTransfer::new(&mut client, &job, 1, &tx).com_extensoes(true, true);
            let mut run = pin!(transfer.download());
            assert!(poll!(&mut run).is_pending());
            assert_eq!(f14a_read(&mut peer).await, PeerMessage::HaveNone);
            assert_eq!(
                f14a_read(&mut peer).await,
                PeerMessage::Extended(0, b"d1:mdee".to_vec())
            );
            assert_eq!(f14a_read(&mut peer).await, PeerMessage::Interested);
            PeerMessage::Extended(0, b"d4:reqqi2ee".to_vec())
                .write_async(&mut peer)
                .await
                .unwrap();
            PeerMessage::HaveAll.write_async(&mut peer).await.unwrap();
            f14b_unchoke(&mut peer).await;
            assert!(poll!(&mut run).is_pending());
            let mut pedidos = 0;
            while let Poll::Ready(message) = poll!(pin!(PeerMessage::read_async(&mut peer))) {
                assert!(matches!(message.unwrap(), PeerMessage::Request { .. }));
                pedidos += 1;
            }
            assert_eq!(pedidos, 2, "reqq=2 deve limitar a fila (que seria 5)");
            println!("HaveNone, Extended, Interested; HaveAll aceito; reqq=2 -> 2 pedidos");
        }
        drop(job);
        let _ = fs::remove_file(dir.join("payload.bin"));
        let _ = fs::remove_dir(dir);
    }

    #[tokio::test(start_paused = true)]
    async fn f15_d_reject_devolve_a_peca_e_reject_inventado_expulsa() {
        let (dir, job, tx) = f15_iniciar("f15-d").await;
        let (mut client, mut peer) = tokio::io::duplex(128 * 1024);
        {
            let mut transfer =
                PeerTransfer::new(&mut client, &job, 1, &tx).com_extensoes(true, false);
            let mut run = pin!(transfer.download());
            assert!(poll!(&mut run).is_pending());
            assert_eq!(f14a_read(&mut peer).await, PeerMessage::HaveNone);
            assert_eq!(f14a_read(&mut peer).await, PeerMessage::Interested);
            PeerMessage::Have(0).write_async(&mut peer).await.unwrap();
            f14b_unchoke(&mut peer).await;
            assert!(poll!(&mut run).is_pending());
            assert_eq!(f14a_read(&mut peer).await, f14b_request(0, 0));
            assert_eq!(f14a_read(&mut peer).await, f14b_request(0, 1));
            PeerMessage::RejectRequest {
                index: 0,
                begin: 0,
                length: BLOCK_SIZE,
            }
            .write_async(&mut peer)
            .await
            .unwrap();
            assert!(poll!(&mut run).is_pending());
            // The other pending block of the rejected piece is cancelled; the piece goes back.
            assert_eq!(
                f14a_read(&mut peer).await,
                PeerMessage::Cancel {
                    index: 0,
                    begin: BLOCK_SIZE,
                    length: BLOCK_SIZE
                }
            );
            assert_ne!(
                job.quadro.lock().unwrap().estado(0),
                Some(&EstadoPeca::Tem),
                "peça rejeitada não pode contar como baixada"
            );
            // The late answer to the cancelled block is not hostile.
            PeerMessage::RejectRequest {
                index: 0,
                begin: BLOCK_SIZE,
                length: BLOCK_SIZE,
            }
            .write_async(&mut peer)
            .await
            .unwrap();
            assert!(poll!(&mut run).is_pending());
            // A Reject for a request never sent closes the connection.
            PeerMessage::RejectRequest {
                index: 1,
                begin: 0,
                length: BLOCK_SIZE,
            }
            .write_async(&mut peer)
            .await
            .unwrap();
            let Poll::Ready(Err(err)) = poll!(&mut run) else {
                panic!("Reject inventado deveria expulsar");
            };
            assert!(err.is::<AparelhoHostil>());
            println!("Reject: Cancel do resto da peça, peça devolvida; Reject inventado: hostil");
        }
        drop(job);
        let _ = fs::remove_file(dir.join("payload.bin"));
        let _ = fs::remove_dir(dir);
    }

    #[tokio::test(start_paused = true)]
    async fn f15_f_have_all_sem_fast_expulsa() {
        let (dir, job, tx) = f15_iniciar("f15-f").await;
        let (mut client, mut peer) = tokio::io::duplex(128 * 1024);
        {
            let mut transfer = PeerTransfer::new(&mut client, &job, 1, &tx);
            let mut run = pin!(transfer.download());
            assert!(poll!(&mut run).is_pending());
            assert_eq!(f14a_read(&mut peer).await, PeerMessage::Interested);
            PeerMessage::HaveAll.write_async(&mut peer).await.unwrap();
            let Poll::Ready(Err(err)) = poll!(&mut run) else {
                panic!("HaveAll sem fast deveria expulsar");
            };
            assert!(err.is::<AparelhoHostil>());
            println!("HaveAll sem a extensão negociada: hostil (BEP 6 manda fechar)");
        }
        drop(job);
        let _ = fs::remove_file(dir.join("payload.bin"));
        let _ = fs::remove_dir(dir);
    }

    #[tokio::test(start_paused = true)]
    async fn f16_pex_so_quando_permitido() {
        // added: 1.2.3.4:6881 and 5.6.7.8:80; added6: [2001:db8::1]:6881.
        let mut pex = b"d5:added12:".to_vec();
        pex.extend_from_slice(&[1, 2, 3, 4, 0x1a, 0xe1, 5, 6, 7, 8, 0, 80]);
        pex.extend_from_slice(b"6:added618:");
        let mut v6 = [0u8; 18];
        v6[..2].copy_from_slice(&[0x20, 0x01]);
        v6[2..4].copy_from_slice(&[0x0d, 0xb8]);
        v6[15] = 1;
        v6[16..].copy_from_slice(&6881u16.to_be_bytes());
        pex.extend_from_slice(&v6);
        pex.push(b'e');
        for permitido in [true, false] {
            let dir = f14a_dir(&format!("f16-{permitido}"));
            let mut job = f14a_job(&dir, 2, 2);
            job.pex = permitido;
            let (tx, mut rx) = mpsc::unbounded_channel();
            let (mut client, mut peer) = tokio::io::duplex(128 * 1024);
            {
                let mut transfer =
                    PeerTransfer::new(&mut client, &job, 1, &tx).com_extensoes(false, true);
                let mut run = pin!(transfer.download());
                assert!(poll!(&mut run).is_pending());
                let hello: &[u8] = if permitido {
                    b"d1:md6:ut_pexi1eee"
                } else {
                    b"d1:mdee"
                };
                assert_eq!(
                    f14a_read(&mut peer).await,
                    PeerMessage::Extended(0, hello.to_vec())
                );
                assert_eq!(f14a_read(&mut peer).await, PeerMessage::Interested);
                PeerMessage::Extended(PEX_ID, pex.clone())
                    .write_async(&mut peer)
                    .await
                    .unwrap();
                assert!(poll!(&mut run).is_pending());
            }
            let mut achados = Vec::new();
            while let Ok(evento) = rx.try_recv() {
                if let EventoAparelho::Descobriu(novos) = evento {
                    achados.extend(novos);
                }
            }
            if permitido {
                let esperado: Vec<std::net::SocketAddr> = vec![
                    "1.2.3.4:6881".parse().unwrap(),
                    "5.6.7.8:80".parse().unwrap(),
                    "[2001:db8::1]:6881".parse().unwrap(),
                ];
                assert_eq!(achados, esperado);
            } else {
                assert!(achados.is_empty(), "PEX em torrent privado: {achados:?}");
            }
            println!("PEX permitido={permitido}: {} endereços", achados.len());
            drop(job);
            let _ = fs::remove_file(dir.join("payload.bin"));
            let _ = fs::remove_dir(dir);
        }
    }

    fn make_info_multi() -> Info {
        // Two files: 20 bytes and 15 bytes. Total 35 bytes.
        // Piece length: 16 bytes.
        // Pieces: 3 pieces. (16, 16, 3)
        let mut data = vec![];
        for i in 0..35 {
            data.push(i as u8);
        }

        let p1 = &data[0..16];
        let p2 = &data[16..32];
        let p3 = &data[32..35];

        let h1: [u8; 20] = Sha1::digest(p1).into();
        let h2: [u8; 20] = Sha1::digest(p2).into();
        let h3: [u8; 20] = Sha1::digest(p3).into();

        Info::MultiFile(MultiFileInfo {
            name: "test_dir".to_string(),
            piece_length: 16,
            pieces: vec![h1, h2, h3],
            files: vec![
                FileInfo {
                    length: 20,
                    path: vec!["f1.bin".to_string()],
                },
                FileInfo {
                    length: 15,
                    path: vec!["f2.bin".to_string()],
                },
            ],
        })
    }

    async fn download_test(
        stream: &mut TcpStream,
        storage: &Arc<TorrentStorage>,
        map: &mut TorrentPieceMap,
        info: &Info,
    ) -> Result<()> {
        let layout = Arc::new(Layout::from_info(info));
        let Info::MultiFile(info) = info else {
            unreachable!()
        };
        let mut quadro = QuadroDePecas::new(
            (0..map.num_pieces)
                .map(|index| tamanho_peca(&layout, index))
                .collect(),
            map,
            128 * 1024 * 1024,
            Some(0),
        )?;
        quadro.entrou(1)?;
        let job = PeerJob {
            quadro: Mutex::new(quadro),
            storage: Arc::clone(storage),
            layout,
            hashes: info.pieces.clone(),
            semaforo: Arc::new(Semaphore::new(1)),
            global_limiter: Arc::new(RateLimiter::new(0)),
            job_limiter: RateLimiter::new(0),
            pex: false,
        };
        let (tx, mut rx) = mpsc::unbounded_channel();
        let result = PeerTransfer::new(stream, &job, 1, &tx).download().await;
        while let Ok(EventoAparelho::Gravou(index)) = rx.try_recv() {
            map.set_piece(index);
        }
        result
    }

    #[tokio::test]
    async fn f13a_limita_conferencias_simultaneas() {
        use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

        let semaforo = Arc::new(Semaphore::new(2));
        let ativos = Arc::new(AtomicUsize::new(0));
        let maximo = Arc::new(AtomicUsize::new(0));
        let trabalhos = (0..6).map(|_| {
            let ativos = Arc::clone(&ativos);
            let maximo = Arc::clone(&maximo);
            conferir_e_gravar(Arc::clone(&semaforo), move || {
                maximo.fetch_max(ativos.fetch_add(1, Relaxed) + 1, Relaxed);
                thread::sleep(Duration::from_millis(50));
                ativos.fetch_sub(1, Relaxed);
                Ok(())
            })
        });
        for trabalho in futures::future::join_all(trabalhos).await {
            trabalho.unwrap();
        }
        let maximo = maximo.load(Relaxed);
        println!("6 trabalhos de 50 ms, N=2, pico={maximo}");
        assert_eq!(maximo, 2);
    }

    #[tokio::test]
    async fn test_peer_transfer_success_and_resume() {
        let dir = std::env::temp_dir().join("chorus_pt_test");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let info = make_info_multi();
        let storage = Arc::new(TorrentStorage::new(&dir, &info).unwrap());

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();

            let msg = PeerMessage::read_from(&mut stream).unwrap();
            assert_eq!(msg, PeerMessage::Interested);

            // Advertise the piece this peer can deliver before disconnecting.
            PeerMessage::Bitfield(vec![0b10000000])
                .write_to(&mut stream)
                .unwrap();
            stream.write_all(&[0, 0, 0, 1, 1]).unwrap();

            // Wait for requests for Piece 0
            let req1 = PeerMessage::read_from(&mut stream).unwrap();
            if let PeerMessage::Request {
                index,
                begin,
                length: _,
            } = req1
            {
                assert_eq!(index, 0);
                let mut data = vec![];
                for i in 0..16 {
                    data.push(i as u8);
                }
                PeerMessage::Piece {
                    index,
                    begin,
                    block: data,
                }
                .write_to(&mut stream)
                .unwrap();
            }

            // Simulate disconnect before piece 1 finishes (or kill simulation)
        });

        let mut stream = TcpStream::connect(addr).await.unwrap();
        let mut map = TorrentPieceMap::new(3);

        // The peer disconnects after piece 0.
        let _ = download_test(&mut stream, &storage, &mut map, &info).await;

        assert!(map.has_piece(0));
        assert!(!map.has_piece(1));
        assert!(!map.has_piece(2));

        handle.join().unwrap();

        // Now test resume!
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let handle2 = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let msg = PeerMessage::read_from(&mut stream).unwrap();
            assert_eq!(msg, PeerMessage::Interested);

            PeerMessage::Bitfield(vec![0b11000000])
                .write_to(&mut stream)
                .unwrap();
            stream.write_all(&[0, 0, 0, 1, 1]).unwrap();

            // Client should request piece 1 next since 0 is done
            let req2 = PeerMessage::read_from(&mut stream).unwrap();
            if let PeerMessage::Request {
                index,
                begin,
                length: _,
            } = req2
            {
                assert_eq!(index, 1);
                let mut data = vec![];
                for i in 16..32 {
                    data.push(i as u8);
                }
                PeerMessage::Piece {
                    index,
                    begin,
                    block: data,
                }
                .write_to(&mut stream)
                .unwrap();
            }

            // Then piece 2, announced after piece 1.
            PeerMessage::Have(2).write_to(&mut stream).unwrap();
            let req3 = PeerMessage::read_from(&mut stream).unwrap();
            if let PeerMessage::Request {
                index,
                begin,
                length: _,
            } = req3
            {
                assert_eq!(index, 2);
                let mut data = vec![];
                for i in 32..35 {
                    data.push(i as u8);
                }
                PeerMessage::Piece {
                    index,
                    begin,
                    block: data,
                }
                .write_to(&mut stream)
                .unwrap();
            }
        });

        let mut stream2 = TcpStream::connect(addr).await.unwrap();
        // Keep the map from before
        download_test(&mut stream2, &storage, &mut map, &info)
            .await
            .unwrap();

        assert!(map.has_piece(0));
        assert!(map.has_piece(1));
        assert!(map.has_piece(2));

        handle2.join().unwrap();
    }

    #[test]
    fn test_peer_transfer_seeding() {
        let dir = std::env::temp_dir().join("chorus_pt_seed_test");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let info = make_info_multi();
        let storage = TorrentStorage::new(&dir, &info).unwrap();

        let expected_hash_0 = match &info {
            Info::MultiFile(m) => m.pieces[0],
            _ => panic!("Expected MultiFile"),
        };

        let mut data_0 = vec![];
        for i in 0..16 {
            data_0.push(i as u8);
        }
        storage
            .write_piece_and_validate(0, &data_0, &expected_hash_0)
            .unwrap();

        let mut map = TorrentPieceMap::new(3);
        map.set_piece(0); // We only have piece 0

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            serve_test_pieces(&mut stream, &storage, &map);
        });

        let mut stream = BlockingStream::connect(addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();

        let msg = PeerMessage::read_from(&mut stream).unwrap();
        assert_eq!(
            msg,
            PeerMessage::read_from(&mut &[0, 0, 0, 1, 1][..]).unwrap()
        );

        PeerMessage::Request {
            index: 0,
            begin: 0,
            length: 16,
        }
        .write_to(&mut stream)
        .unwrap();

        let msg = PeerMessage::read_from(&mut stream).unwrap();
        if let PeerMessage::Piece {
            index,
            begin,
            block,
        } = msg
        {
            assert_eq!(index, 0);
            assert_eq!(begin, 0);
            assert_eq!(block.len(), 16);
            assert_eq!(block, data_0);
        } else {
            panic!("Expected piece");
        }

        // Request a piece they don't have
        PeerMessage::Request {
            index: 1,
            begin: 0,
            length: 16,
        }
        .write_to(&mut stream)
        .unwrap();

        // Should timeout because they don't respond
        let res = PeerMessage::read_from(&mut stream);
        assert!(res.is_err());

        drop(stream);
        let _ = handle.join();
    }

    #[tokio::test]
    async fn test_peer_transfer_full_leech() {
        let dir_seed = std::env::temp_dir().join("chorus_pt_leech_seed");
        let dir_leech = std::env::temp_dir().join("chorus_pt_leech_client");
        let _ = fs::remove_dir_all(&dir_seed);
        let _ = fs::remove_dir_all(&dir_leech);
        fs::create_dir_all(&dir_seed).unwrap();
        fs::create_dir_all(&dir_leech).unwrap();

        let info = make_info_multi();
        let (num_pieces, _piece_len, _total_len) = match &info {
            Info::MultiFile(m) => (m.pieces.len(), m.piece_length, 35),
            _ => panic!("Expected MultiFile"),
        };

        // Initialize Seeder storage with all pieces
        let storage_seed = TorrentStorage::new(&dir_seed, &info).unwrap();
        let mut map_seed = TorrentPieceMap::new(num_pieces);
        let mut data = vec![];
        for i in 0..35 {
            data.push(i as u8);
        }
        let pieces = match &info {
            Info::MultiFile(m) => &m.pieces,
            _ => panic!("Expected MultiFile"),
        };
        storage_seed
            .write_piece_and_validate(0, &data[0..16], &pieces[0])
            .unwrap();
        storage_seed
            .write_piece_and_validate(1, &data[16..32], &pieces[1])
            .unwrap();
        storage_seed
            .write_piece_and_validate(2, &data[32..35], &pieces[2])
            .unwrap();
        map_seed.set_piece(0);
        map_seed.set_piece(1);
        map_seed.set_piece(2);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            PeerMessage::Bitfield(map_seed.bitfield.clone())
                .write_to(&mut stream)
                .unwrap();
            serve_test_pieces(&mut stream, &storage_seed, &map_seed);
        });

        // Initialize Leecher storage with 0 pieces
        let storage_leech = Arc::new(TorrentStorage::new(&dir_leech, &info).unwrap());
        let mut map_leech = TorrentPieceMap::new(num_pieces);
        let mut stream_leech = TcpStream::connect(addr).await.unwrap();
        download_test(&mut stream_leech, &storage_leech, &mut map_leech, &info)
            .await
            .unwrap();

        // Validate Leecher got everything
        assert!(map_leech.has_piece(0));
        assert!(map_leech.has_piece(1));
        assert!(map_leech.has_piece(2));

        drop(stream_leech);
        let _ = handle.join();
        let _ = fs::remove_dir_all(&dir_seed);
        let _ = fs::remove_dir_all(&dir_leech);
    }

    fn serve_test_pieces(
        stream: &mut BlockingStream,
        storage: &TorrentStorage,
        map: &TorrentPieceMap,
    ) {
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream.write_all(&[0, 0, 0, 1, 1]).unwrap();
        while let Ok(message) = PeerMessage::read_from(stream) {
            if let PeerMessage::Request {
                index,
                begin,
                length,
            } = message
                && map.has_piece(index as usize)
            {
                let offset = begin as usize;
                let mut data = vec![0; offset + length as usize];
                storage.read_piece(index as usize, &mut data).unwrap();
                PeerMessage::Piece {
                    index,
                    begin,
                    block: data[offset..offset + length as usize].to_vec(),
                }
                .write_to(stream)
                .unwrap();
            }
        }
    }
}
