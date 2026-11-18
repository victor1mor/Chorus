//! BEP 19 web seeds: pieces fetched over HTTP(S) by byte range, through the protected client.
//!
//! A web seed acts as one more "aparelho" on the board: it has every piece, asks the board
//! for work, and every piece is SHA-1 checked before it reaches the disk.

use crate::http::HttpFetcher;
use crate::peer_transfer::{EventoAparelho, PeerJob, conferir_e_gravar_peca, tamanho_peca};
use crate::quadro::AparelhoId;
use crate::torrent_storage::LayoutFile;
use anyhow::{Result, anyhow, ensure};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep};
use url::Url;

/// Consecutive HTTP failures before this web seed is dropped for the round.
const MAX_FALHAS: u32 = 3;
/// Most web seeds taken from one torrent.
pub(crate) const MAX_SEMEADORES_WEB: usize = 10;

/// `url-list` entries usable as web seeds: http(s) only, at most `MAX_SEMEADORES_WEB`.
pub(crate) fn semeadores_validos(urls: &[String], rede_protegida: bool) -> Vec<Url> {
    urls.iter()
        .filter(|url| crate::rede::validar_url(url, rede_protegida).is_ok())
        .filter_map(|url| Url::parse(url).ok())
        .filter(|url| matches!(url.scheme(), "http" | "https"))
        .take(MAX_SEMEADORES_WEB)
        .collect()
}

/// BEP 19 file URL. Single file: the URL itself, or URL + name when it ends in `/`.
/// Multi-file: the URL is a root folder; append name and path, each segment percent-encoded.
pub(crate) fn url_do_arquivo(base: &Url, arquivo: &LayoutFile, multi: bool) -> Option<Url> {
    if !multi && !base.path().ends_with('/') {
        return Some(base.clone());
    }
    let mut url = base.clone();
    url.path_segments_mut()
        .ok()?
        .pop_if_empty()
        .extend(&arquivo.path_components);
    Some(url)
}

async fn buscar_peca(
    fetcher: &HttpFetcher,
    base: &Url,
    job: &PeerJob,
    peca: usize,
) -> Result<Vec<u8>> {
    let layout = &job.layout;
    let inicio = peca as u64 * layout.piece_length;
    let fim = inicio + tamanho_peca(layout, peca);
    let multi = layout.files.len() > 1;
    let mut dados = Vec::with_capacity((fim - inicio) as usize);
    for arquivo in &layout.files {
        let a = inicio.max(arquivo.offset_in_torrent);
        let b = fim.min(arquivo.offset_in_torrent + arquivo.length);
        if a >= b {
            continue;
        }
        let url = url_do_arquivo(base, arquivo, multi)
            .ok_or_else(|| anyhow!("web seed URL cannot hold a path"))?;
        let (de, ate) = (
            a - arquivo.offset_in_torrent,
            b - arquivo.offset_in_torrent - 1,
        );
        // fetch_chunk demands 206 and the exact Content-Range, so the body length is known.
        let mut resposta = fetcher
            .fetch_chunk(url.as_str(), de, ate, arquivo.length, None)
            .await?;
        let esperado = (b - a) as usize;
        let antes = dados.len();
        while let Some(pedaco) = resposta.chunk().await? {
            ensure!(
                dados.len() - antes + pedaco.len() <= esperado,
                "web seed sent more than the requested range"
            );
            dados.extend_from_slice(&pedaco);
        }
        ensure!(
            dados.len() - antes == esperado,
            "web seed sent a short range"
        );
    }
    Ok(dados)
}

/// Runs one web seed until the torrent is complete, it fails `MAX_FALHAS` times in a row,
/// or it sends a piece with the wrong SHA-1 (BEP 19: discard that URL).
pub(crate) async fn semear_web(
    base: Url,
    fetcher: HttpFetcher,
    id: AparelhoId,
    job: Arc<PeerJob>,
    eventos: mpsc::UnboundedSender<EventoAparelho>,
) -> Result<()> {
    {
        let pecas = job.hashes.len();
        let mut bits = vec![0xff_u8; pecas.div_ceil(8)];
        if !pecas.is_multiple_of(8) {
            *bits.last_mut().unwrap() = 0xff << (8 - pecas % 8);
        }
        let mut quadro = job.quadro.lock().unwrap();
        quadro.entrou(id)?;
        quadro.bitfield(id, &bits)?;
    }
    let resultado = async {
        let mut falhas = 0;
        loop {
            let atribuicao = {
                let mut quadro = job.quadro.lock().unwrap();
                if quadro.completo() {
                    return Ok(());
                }
                quadro.pedir(id, Instant::now())
            };
            let Some(atribuicao) = atribuicao else {
                // Every missing piece is with someone else right now.
                sleep(Duration::from_millis(500)).await;
                continue;
            };
            let peca = atribuicao.peca;
            let bytes = tamanho_peca(&job.layout, peca);
            job.global_limiter.wait_for(bytes).await;
            job.job_limiter.wait_for(bytes).await;
            let dados = match buscar_peca(&fetcher, &base, &job, peca).await {
                Ok(dados) => dados,
                Err(err) => {
                    let _ = job.quadro.lock().unwrap().soltar(id, peca);
                    falhas += 1;
                    if falhas >= MAX_FALHAS {
                        return Err(err.context("web seed failed 3 times in a row"));
                    }
                    sleep(Duration::from_secs(1 << falhas)).await;
                    continue;
                }
            };
            falhas = 0;
            let ok = conferir_e_gravar_peca(&job, peca, dados).await?;
            let concluiu = job
                .quadro
                .lock()
                .unwrap()
                .concluir(id, peca, ok, Instant::now())
                .is_ok();
            if !ok {
                return Err(anyhow!("web seed sent a piece with the wrong SHA-1"));
            }
            if concluiu {
                let _ = eventos.send(EventoAparelho::Gravou(peca));
            }
        }
    }
    .await;
    let _ = job.quadro.lock().unwrap().saiu(id);
    resultado
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arquivo(componentes: &[&str]) -> LayoutFile {
        LayoutFile {
            length: 1,
            path_components: componentes.iter().map(|c| c.to_string()).collect(),
            offset_in_torrent: 0,
        }
    }

    #[test]
    fn f18_url_do_arquivo_segue_a_bep19() {
        let unico = arquivo(&["filme.mkv"]);
        let direto = Url::parse("http://espelho.example/pub/f.mkv").unwrap();
        assert_eq!(url_do_arquivo(&direto, &unico, false).unwrap(), direto);
        let pasta = Url::parse("http://espelho.example/pub/").unwrap();
        assert_eq!(
            url_do_arquivo(&pasta, &unico, false).unwrap().as_str(),
            "http://espelho.example/pub/filme.mkv"
        );
        let multi = arquivo(&["michael", "sub dir", "Readme#1.txt"]);
        assert_eq!(
            url_do_arquivo(&pasta, &multi, true).unwrap().as_str(),
            "http://espelho.example/pub/michael/sub%20dir/Readme%231.txt"
        );
        let sem_barra = Url::parse("http://espelho.example/pub").unwrap();
        assert_eq!(
            url_do_arquivo(&sem_barra, &multi, true).unwrap().as_str(),
            "http://espelho.example/pub/michael/sub%20dir/Readme%231.txt"
        );
        println!("BEP 19: arquivo único direto e por pasta; multi com nome + caminho codificados");
    }

    #[test]
    fn f18_so_http_https_e_rede_protegida() {
        let urls = [
            "ftp://espelho.example/f".to_string(),
            "http://espelho.example/f".to_string(),
            "https://espelho.example/f".to_string(),
            "http://127.0.0.1/f".to_string(),
            "não é url".to_string(),
        ];
        assert_eq!(semeadores_validos(&urls, false).len(), 3);
        let protegida: Vec<_> = semeadores_validos(&urls, true)
            .into_iter()
            .map(String::from)
            .collect();
        assert_eq!(
            protegida,
            ["http://espelho.example/f", "https://espelho.example/f"]
        );
        let muitos: Vec<_> = (0..20).map(|i| format!("http://e{i}.example/f")).collect();
        assert_eq!(semeadores_validos(&muitos, false).len(), MAX_SEMEADORES_WEB);
        println!("web seeds: só http(s), loopback fora na rede protegida, teto de 10");
    }
}
