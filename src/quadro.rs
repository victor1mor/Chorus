use crate::torrent_storage::TorrentPieceMap;
use anyhow::{Result, anyhow, ensure};
use std::collections::{HashMap, VecDeque, hash_map::Entry};
use tokio::time::Instant;

pub(crate) type AparelhoId = u64;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum EstadoPeca {
    Tem,
    EmVoo {
        dono: AparelhoId,
        inicio: Instant,
        ajudantes: Vec<(AparelhoId, Instant)>,
    },
    NaFila,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Atribuicao {
    pub peca: usize,
    pub roubada_de: Option<AparelhoId>,
    pub modo_final: bool,
}

struct Peca {
    tamanho: u64,
    estado: EstadoPeca,
    disponibilidade: usize,
    atrasados: Vec<AparelhoId>,
}

struct Aparelho {
    mapa: TorrentPieceMap,
    anunciou: bool,
}

pub(crate) struct QuadroDePecas {
    pecas: Vec<Peca>,
    aparelhos: HashMap<AparelhoId, Aparelho>,
    orcamento_bytes: u64,
    bytes_em_voo: u64,
    sorteios_restantes: u8,
    duracoes_ns: VecDeque<u128>,
    semente: u64,
}

impl QuadroDePecas {
    pub(crate) fn new(
        tamanhos_das_pecas: Vec<u64>,
        retomada: &TorrentPieceMap,
        orcamento_bytes: u64,
        semente: Option<u64>,
    ) -> Result<Self> {
        ensure!(
            retomada.num_pieces == tamanhos_das_pecas.len(),
            "mapa de retomada com número de peças incorreto"
        );
        let semente = match semente {
            Some(semente) => semente,
            None => {
                let mut bytes = [0; 8];
                getrandom::fill(&mut bytes)
                    .map_err(|erro| anyhow!("não foi possível sortear a semente: {erro}"))?;
                u64::from_le_bytes(bytes)
            }
        };
        let pecas = tamanhos_das_pecas
            .into_iter()
            .enumerate()
            .map(|(indice, tamanho)| Peca {
                tamanho,
                estado: if retomada.has_piece(indice) {
                    EstadoPeca::Tem
                } else {
                    EstadoPeca::NaFila
                },
                disponibilidade: 0,
                atrasados: Vec::new(),
            })
            .collect();
        Ok(Self {
            pecas,
            aparelhos: HashMap::new(),
            orcamento_bytes,
            bytes_em_voo: 0,
            sorteios_restantes: 4,
            duracoes_ns: VecDeque::with_capacity(20),
            semente,
        })
    }

    pub(crate) fn entrou(&mut self, aparelho: AparelhoId) -> Result<()> {
        let Entry::Vacant(entrada) = self.aparelhos.entry(aparelho) else {
            anyhow::bail!("aparelho já conectado");
        };
        entrada.insert(Aparelho {
            mapa: TorrentPieceMap::new(self.pecas.len()),
            anunciou: false,
        });
        Ok(())
    }

    pub(crate) fn bitfield(&mut self, aparelho: AparelhoId, bits: &[u8]) -> Result<()> {
        let aparelho = self
            .aparelhos
            .get_mut(&aparelho)
            .ok_or_else(|| anyhow!("aparelho desconhecido"))?;
        ensure!(!aparelho.anunciou, "bitfield após anúncio de peças");
        ensure!(
            bits.len() == self.pecas.len().div_ceil(8),
            "bitfield de tamanho incorreto"
        );
        let mapa = TorrentPieceMap {
            bitfield: bits.to_vec(),
            num_pieces: self.pecas.len(),
        };
        ensure!(
            mapa.matches_piece_count(self.pecas.len()),
            "bitfield de tamanho incorreto ou com bits de sobra"
        );
        for (indice, peca) in self.pecas.iter_mut().enumerate() {
            peca.disponibilidade += usize::from(mapa.has_piece(indice));
        }
        aparelho.mapa = mapa;
        aparelho.anunciou = true;
        Ok(())
    }

    pub(crate) fn have(&mut self, aparelho: AparelhoId, indice: usize) -> Result<()> {
        let aparelho = self
            .aparelhos
            .get_mut(&aparelho)
            .ok_or_else(|| anyhow!("aparelho desconhecido"))?;
        let peca = self
            .pecas
            .get_mut(indice)
            .ok_or_else(|| anyhow!("peça fora do quadro"))?;
        aparelho.anunciou = true;
        if !aparelho.mapa.has_piece(indice) {
            aparelho.mapa.set_piece(indice);
            peca.disponibilidade += 1;
        }
        Ok(())
    }

    pub(crate) fn pedir(&mut self, aparelho: AparelhoId, agora: Instant) -> Option<Atribuicao> {
        // ponytail: varreduras lineares por pedido; indexar a fila e as peças em voo se a medição exigir.
        self.roubar(aparelho, agora, 10)
            .or_else(|| self.pedir_da_fila(aparelho, agora, false))
            .or_else(|| self.roubar(aparelho, agora, 3))
            .or_else(|| self.pedir_no_modo_final(aparelho, agora))
            .or_else(|| self.pedir_da_fila(aparelho, agora, true))
    }

    fn bloqueado(&self, peca: usize, aparelho: AparelhoId) -> bool {
        let atrasados = &self.pecas[peca].atrasados;
        atrasados.contains(&aparelho)
            && self.aparelhos.iter().any(|(&outro, registro)| {
                outro != aparelho && registro.mapa.has_piece(peca) && !atrasados.contains(&outro)
            })
    }

    fn roubar(&mut self, aparelho: AparelhoId, agora: Instant, fator: u128) -> Option<Atribuicao> {
        let total = self.duracoes_ns.len() as u128;
        if total == 0 {
            return None;
        }
        let limite = self.duracoes_ns.iter().sum::<u128>() * fator;
        let mapa = &self.aparelhos.get(&aparelho)?.mapa;
        for (peca, registro) in self.pecas.iter_mut().enumerate() {
            if let EstadoPeca::EmVoo {
                dono,
                inicio,
                ajudantes,
            } = &mut registro.estado
                && *dono != aparelho
                && mapa.has_piece(peca)
                && !registro.atrasados.contains(&aparelho)
                && !ajudantes.iter().any(|&(id, _)| id == aparelho)
                && agora.duration_since(*inicio).as_nanos() * total > limite
            {
                *inicio = agora;
                return Some(Atribuicao {
                    peca,
                    roubada_de: Some(std::mem::replace(dono, aparelho)),
                    modo_final: false,
                });
            }
        }
        None
    }

    fn pedir_da_fila(
        &mut self,
        aparelho: AparelhoId,
        agora: Instant,
        ultimo_recurso: bool,
    ) -> Option<Atribuicao> {
        let mapa = &self.aparelhos.get(&aparelho)?.mapa;
        let mut melhor = usize::MAX;
        let mut empates = 0;
        let mut escolhida = None;
        for (indice, peca) in self.pecas.iter().enumerate() {
            if peca.estado != EstadoPeca::NaFila
                || !mapa.has_piece(indice)
                || !self.cabe(peca.tamanho)
                || peca.atrasados.contains(&aparelho) != ultimo_recurso
                || (ultimo_recurso && self.bloqueado(indice, aparelho))
            {
                continue;
            }
            let raridade = if self.sorteios_restantes > 0 {
                0
            } else {
                peca.disponibilidade
            };
            if raridade < melhor {
                melhor = raridade;
                empates = 0;
            }
            if raridade == melhor {
                empates += 1;
                if Self::sortear(&mut self.semente).is_multiple_of(empates) {
                    escolhida = Some(indice);
                }
            }
        }
        let peca = escolhida?;
        self.sorteios_restantes = self.sorteios_restantes.saturating_sub(1);
        let registro = &mut self.pecas[peca];
        registro.estado = EstadoPeca::EmVoo {
            dono: aparelho,
            inicio: agora,
            ajudantes: Vec::new(),
        };
        self.bytes_em_voo += registro.tamanho;
        Some(Atribuicao {
            peca,
            roubada_de: None,
            modo_final: false,
        })
    }

    fn pedir_no_modo_final(&mut self, aparelho: AparelhoId, agora: Instant) -> Option<Atribuicao> {
        let mapa = &self.aparelhos.get(&aparelho)?.mapa;
        if self
            .pecas
            .iter()
            .any(|peca| peca.estado == EstadoPeca::NaFila)
        {
            return None;
        }
        let (peca, _) = self
            .pecas
            .iter()
            .enumerate()
            .filter_map(|(indice, peca)| match &peca.estado {
                EstadoPeca::EmVoo {
                    dono, ajudantes, ..
                } if *dono != aparelho
                    && !ajudantes.iter().any(|&(id, _)| id == aparelho)
                    && mapa.has_piece(indice)
                    && !peca.atrasados.contains(&aparelho)
                    && self.cabe(peca.tamanho) =>
                {
                    Some((indice, ajudantes.len()))
                }
                _ => None,
            })
            .min_by_key(|&(_, ajudantes)| ajudantes)?;
        let registro = &mut self.pecas[peca];
        let EstadoPeca::EmVoo { ajudantes, .. } = &mut registro.estado else {
            unreachable!("a peça selecionada está em voo");
        };
        ajudantes.push((aparelho, agora));
        self.bytes_em_voo += registro.tamanho;
        Some(Atribuicao {
            peca,
            roubada_de: None,
            modo_final: true,
        })
    }

    fn cabe(&self, tamanho: u64) -> bool {
        self.bytes_em_voo == 0
            || self
                .bytes_em_voo
                .checked_add(tamanho)
                .is_some_and(|total| total <= self.orcamento_bytes)
    }

    fn sortear(semente: &mut u64) -> u64 {
        // SplitMix64
        *semente = semente.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut valor = *semente;
        valor = (valor ^ (valor >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        valor = (valor ^ (valor >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        valor ^ (valor >> 31)
    }

    pub(crate) fn concluir(
        &mut self,
        aparelho: AparelhoId,
        peca: usize,
        hash_ok: bool,
        agora: Instant,
    ) -> Result<Vec<AparelhoId>> {
        if !hash_ok {
            self.soltar(aparelho, peca)?;
            return Ok(Vec::new());
        }
        let registro = self
            .pecas
            .get_mut(peca)
            .ok_or_else(|| anyhow!("peça fora do quadro"))?;
        let EstadoPeca::EmVoo {
            dono,
            inicio,
            ajudantes,
        } = &mut registro.estado
        else {
            anyhow::bail!("peça fora de voo");
        };
        ensure!(
            *dono == aparelho || ajudantes.iter().any(|&(id, _)| id == aparelho),
            "aparelho sem atribuição para a peça"
        );
        let duracao = agora.duration_since(*inicio).as_nanos();
        self.bytes_em_voo -= registro.tamanho * (1 + ajudantes.len() as u64);
        let cancelar = ajudantes
            .iter()
            .map(|&(id, _)| id)
            .chain(std::iter::once(*dono))
            .filter(|&id| id != aparelho)
            .collect();
        registro.estado = EstadoPeca::Tem;
        registro.atrasados = Vec::new();
        if self.duracoes_ns.len() == 20 {
            self.duracoes_ns.pop_front();
        }
        self.duracoes_ns.push_back(duracao);
        Ok(cancelar)
    }

    pub(crate) fn soltar(&mut self, aparelho: AparelhoId, peca: usize) -> Result<()> {
        ensure!(
            self.liberar(aparelho, peca),
            "aparelho sem atribuição para a peça"
        );
        Ok(())
    }

    pub(crate) fn soltar_por_atraso(&mut self, aparelho: AparelhoId, peca: usize) -> Result<()> {
        self.soltar(aparelho, peca)?;
        let atrasados = &mut self.pecas[peca].atrasados;
        if !atrasados.contains(&aparelho) {
            atrasados.push(aparelho);
        }
        Ok(())
    }

    fn liberar(&mut self, aparelho: AparelhoId, peca: usize) -> bool {
        let Some(registro) = self.pecas.get_mut(peca) else {
            return false;
        };
        let EstadoPeca::EmVoo {
            dono,
            inicio,
            ajudantes,
        } = &mut registro.estado
        else {
            return false;
        };
        if *dono == aparelho {
            if let Some(ajudante) = ajudantes.pop() {
                (*dono, *inicio) = ajudante;
            } else {
                registro.estado = EstadoPeca::NaFila;
            }
        } else if let Some(indice) = ajudantes.iter().position(|&(id, _)| id == aparelho) {
            ajudantes.swap_remove(indice);
        } else {
            return false;
        }
        self.bytes_em_voo -= registro.tamanho;
        true
    }

    pub(crate) fn saiu(&mut self, aparelho: AparelhoId) -> Result<()> {
        let removido = self
            .aparelhos
            .remove(&aparelho)
            .ok_or_else(|| anyhow!("aparelho desconhecido"))?;
        for indice in 0..self.pecas.len() {
            if removido.mapa.has_piece(indice) {
                self.pecas[indice].disponibilidade -= 1;
                self.pecas[indice].atrasados.retain(|&id| id != aparelho);
                self.liberar(aparelho, indice);
            }
        }
        Ok(())
    }

    pub(crate) fn estado(&self, peca: usize) -> Option<&EstadoPeca> {
        self.pecas.get(peca).map(|peca| &peca.estado)
    }

    pub(crate) fn completo(&self) -> bool {
        self.pecas.iter().all(|peca| peca.estado == EstadoPeca::Tem)
    }

    #[cfg(test)]
    pub(crate) fn bytes_em_voo(&self) -> u64 {
        self.bytes_em_voo
    }

    #[cfg(test)]
    pub(crate) fn disponibilidade(&self, peca: usize) -> Option<usize> {
        self.pecas.get(peca).map(|peca| peca.disponibilidade)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::HashSet;
    use std::time::Duration;

    fn quadro(tamanhos: Vec<u64>, orcamento: u64, semente: u64) -> QuadroDePecas {
        let mapa = TorrentPieceMap::new(tamanhos.len());
        QuadroDePecas::new(tamanhos, &mapa, orcamento, Some(semente)).unwrap()
    }

    fn anunciar(quadro: &mut QuadroDePecas, aparelho: AparelhoId, pecas: &[usize]) {
        quadro.entrou(aparelho).unwrap();
        for &peca in pecas {
            quadro.have(aparelho, peca).unwrap();
        }
    }

    #[test]
    fn a_valida_bitfield_e_have() {
        let mut quadro = quadro(vec![1; 9], 9, 0);
        quadro.entrou(1).unwrap();
        assert!(quadro.entrou(1).is_err());
        assert!(quadro.bitfield(1, &[0]).is_err());
        assert!(quadro.bitfield(1, &[0, 1]).is_err());
        quadro.bitfield(1, &[0x81, 0x80]).unwrap();
        assert!(quadro.bitfield(1, &[0, 0]).is_err());
        for peca in 0..9 {
            assert_eq!(
                quadro.disponibilidade(peca),
                Some(usize::from([0, 7, 8].contains(&peca)))
            );
        }
        quadro.have(1, 8).unwrap();
        assert_eq!(quadro.disponibilidade(8), Some(1));
        anunciar(&mut quadro, 2, &[1]);
        assert!(quadro.bitfield(2, &[0, 0]).is_err());
        assert!(quadro.have(2, 9).is_err());
    }

    #[test]
    fn b_quatro_sorteios_depois_raridade() {
        let agora = Instant::now();
        let mut primeiras = HashSet::new();
        let mut ignorou_raridade = [false; 4];
        for semente in 0..100 {
            let mut quadro = quadro(vec![1; 8], 8, semente);
            anunciar(&mut quadro, 1, &[0, 1, 2, 3, 4, 5, 6, 7]);
            anunciar(&mut quadro, 2, &[1, 2, 3, 4, 5, 6, 7]);
            for (posicao, ignorou) in ignorou_raridade.iter_mut().enumerate() {
                let rara_na_fila = quadro.estado(0) == Some(&EstadoPeca::NaFila);
                let peca = quadro.pedir(1, agora).unwrap().peca;
                if posicao == 0 {
                    primeiras.insert(peca);
                }
                *ignorou |= rara_na_fila && peca != 0;
            }
        }
        assert!(primeiras.len() > 1);
        assert!(ignorou_raridade.into_iter().all(|ignorou| ignorou));

        let mut quadro = quadro(vec![1; 8], 8, 7);
        anunciar(&mut quadro, 1, &[0, 1, 2, 3, 4, 5, 6, 7]);
        for _ in 0..4 {
            quadro.pedir(1, agora).unwrap();
        }
        let na_fila: Vec<_> = (0..8)
            .filter(|&peca| quadro.estado(peca) == Some(&EstadoPeca::NaFila))
            .collect();
        anunciar(&mut quadro, 2, &na_fila[..3]);
        assert_eq!(quadro.pedir(1, agora).unwrap().peca, na_fila[3]);
    }

    #[test]
    fn c_modo_final_exige_fila_vazia_no_job() {
        let agora = Instant::now();
        let mut quadro = quadro(vec![1; 2], 5, 0);
        anunciar(&mut quadro, 1, &[0]);
        anunciar(&mut quadro, 2, &[0]);
        assert_eq!(quadro.pedir(1, agora).unwrap().peca, 0);
        assert_eq!(quadro.estado(1), Some(&EstadoPeca::NaFila));
        assert_eq!(quadro.pedir(2, agora), None);

        quadro.have(1, 1).unwrap();
        assert_eq!(quadro.pedir(1, agora).unwrap().peca, 1);
        assert_eq!(
            quadro.pedir(2, agora),
            Some(Atribuicao {
                peca: 0,
                roubada_de: None,
                modo_final: true
            })
        );
        assert_eq!(quadro.pedir(2, agora), None);
        anunciar(&mut quadro, 3, &[0, 1]);
        assert_eq!(quadro.pedir(3, agora).unwrap().peca, 1);
        anunciar(&mut quadro, 4, &[0]);
        assert!(quadro.pedir(4, agora).unwrap().modo_final);
        let cancelar = quadro.concluir(2, 0, true, agora).unwrap();
        assert_eq!(cancelar.len(), 2);
        assert!(cancelar.contains(&1) && cancelar.contains(&4));
        assert_eq!(quadro.pedir(2, agora), None);
    }

    #[test]
    fn d_roubo_10x_antes_da_fila_e_3x_depois() {
        let inicio = Instant::now();
        for segundos in [11, 4] {
            let mut quadro = quadro(vec![1; 3], 2, 0);
            anunciar(&mut quadro, 1, &[1]);
            anunciar(&mut quadro, 2, &[1]);
            anunciar(&mut quadro, 3, &[1]);
            assert_eq!(quadro.pedir(1, inicio).unwrap().peca, 1);
            let agora = inicio + Duration::from_secs(segundos);
            quadro.have(1, 0).unwrap();
            assert_eq!(
                quadro
                    .pedir(1, agora - Duration::from_secs(1))
                    .unwrap()
                    .peca,
                0
            );
            assert_eq!(quadro.pedir(3, agora), None);
            quadro.concluir(1, 0, true, agora).unwrap();
            quadro.have(2, 2).unwrap();
            if segundos == 4 {
                assert_eq!(quadro.pedir(2, agora).unwrap().peca, 2);
            }
            let bytes = quadro.bytes_em_voo();
            assert_eq!(
                quadro.pedir(2, agora),
                Some(Atribuicao {
                    peca: 1,
                    roubada_de: Some(1),
                    modo_final: false
                })
            );
            assert_eq!(quadro.bytes_em_voo(), bytes);
            assert_eq!(
                quadro.estado(1),
                Some(&EstadoPeca::EmVoo {
                    dono: 2,
                    inicio: agora,
                    ajudantes: vec![]
                })
            );
        }

        let mut media = quadro(vec![1; 23], 1, 0);
        media.entrou(1).unwrap();
        let mut agora = inicio;
        for peca in 0..21 {
            media.have(1, peca).unwrap();
            assert_eq!(media.pedir(1, agora).unwrap().peca, peca);
            agora += Duration::from_secs(if peca == 0 { 100 } else { 1 });
            media.concluir(1, peca, true, agora).unwrap();
        }
        media.have(1, 21).unwrap();
        anunciar(&mut media, 2, &[21]);
        media.pedir(1, agora).unwrap();
        assert_eq!(
            media
                .pedir(2, agora + Duration::from_secs(4))
                .unwrap()
                .roubada_de,
            Some(1)
        );
    }

    #[test]
    fn e_orcamento_inclui_ajudantes_e_admite_uma_peca_sozinha() {
        let agora = Instant::now();
        let mut fila = quadro(vec![8; 3], 16, 0);
        anunciar(&mut fila, 1, &[0, 1, 2]);
        assert!(fila.pedir(1, agora).is_some());
        assert!(fila.pedir(1, agora).is_some());
        assert_eq!(fila.pedir(1, agora), None);
        assert_eq!(fila.bytes_em_voo(), 16);

        let mut final_do_job = quadro(vec![8], 16, 0);
        for aparelho in 1..=3 {
            anunciar(&mut final_do_job, aparelho, &[0]);
        }
        assert!(!final_do_job.pedir(1, agora).unwrap().modo_final);
        assert!(final_do_job.pedir(2, agora).unwrap().modo_final);
        assert_eq!(final_do_job.pedir(3, agora), None);
        assert_eq!(final_do_job.bytes_em_voo(), 16);

        let mut grande = quadro(vec![17], 16, 0);
        anunciar(&mut grande, 1, &[0]);
        anunciar(&mut grande, 2, &[0]);
        assert_eq!(grande.pedir(1, agora).unwrap().peca, 0);
        assert_eq!(grande.bytes_em_voo(), 17);
        assert_eq!(grande.pedir(2, agora), None);
    }

    #[test]
    fn f_hash_falho_e_retomada() {
        let agora = Instant::now();
        let mut retomada = TorrentPieceMap::new(10);
        for peca in 0..4 {
            retomada.set_piece(peca);
        }
        assert!(QuadroDePecas::new(vec![1; 9], &retomada, 10, Some(7)).is_err());
        let mut com_retomada = QuadroDePecas::new(vec![1; 10], &retomada, 10, Some(7)).unwrap();
        let mut sem_retomada = quadro(vec![1; 10], 10, 7);
        for quadro in [&mut com_retomada, &mut sem_retomada] {
            anunciar(quadro, 1, &[4, 5, 6, 7, 8, 9]);
            anunciar(quadro, 2, &[5, 6, 7, 8, 9]);
        }
        for peca in 0..4 {
            assert_eq!(com_retomada.estado(peca), Some(&EstadoPeca::Tem));
            assert!(com_retomada.concluir(1, peca, false, agora).is_err());
        }
        for _ in 0..4 {
            assert_eq!(com_retomada.pedir(1, agora), sem_retomada.pedir(1, agora));
        }

        let mut falha = quadro(vec![1], 2, 0);
        anunciar(&mut falha, 1, &[0]);
        anunciar(&mut falha, 2, &[0]);
        falha.pedir(1, agora).unwrap();
        assert!(falha.concluir(2, 0, false, agora).is_err());
        assert!(falha.concluir(1, 0, false, agora).unwrap().is_empty());
        assert_eq!(falha.estado(0), Some(&EstadoPeca::NaFila));
        falha.pedir(1, agora).unwrap();
        falha.pedir(2, agora).unwrap();
        assert!(falha.concluir(1, 0, false, agora).unwrap().is_empty());
        assert_eq!(
            falha.estado(0),
            Some(&EstadoPeca::EmVoo {
                dono: 2,
                inicio: agora,
                ajudantes: vec![]
            })
        );
        assert_eq!(falha.bytes_em_voo(), 1);
        assert!(falha.concluir(2, 0, true, agora).unwrap().is_empty());
        assert!(falha.completo());
    }

    #[test]
    fn f14a_atraso_bloqueia_fila_modo_final_e_roubo_ate_tem_ou_saida() {
        let inicio = Instant::now();
        let mut quadro = quadro(vec![1; 3], 3, 0);
        anunciar(&mut quadro, 1, &[0]);
        anunciar(&mut quadro, 2, &[0, 1, 2]);
        assert_eq!(quadro.pedir(1, inicio).unwrap().peca, 0);
        quadro.soltar_por_atraso(1, 0).unwrap();
        assert_eq!(quadro.estado(0), Some(&EstadoPeca::NaFila));
        assert_eq!(quadro.pedir(1, inicio), None, "voltou pela fila");
        quadro.have(1, 1).unwrap();
        assert_eq!(quadro.pedir(1, inicio).unwrap().peca, 1);
        let agora = inicio + Duration::from_secs(1);
        quadro.concluir(1, 1, true, agora).unwrap();

        quadro.pedir(2, agora).unwrap();
        quadro.pedir(2, agora).unwrap();
        assert_eq!(quadro.pedir(1, agora), None, "voltou no modo final");
        let depois = agora + Duration::from_secs(11);
        assert_eq!(quadro.pedir(1, depois), None, "voltou por roubo");

        quadro.saiu(1).unwrap();
        anunciar(&mut quadro, 1, &[0, 2]);
        assert!(quadro.pedir(1, agora).unwrap().modo_final);
        quadro.soltar_por_atraso(1, 0).unwrap();
        assert_eq!(quadro.bytes_em_voo(), 2);
        quadro.concluir(2, 0, true, depois).unwrap();
        assert!(quadro.pecas[0].atrasados.is_empty());
    }

    #[test]
    fn f14a_roubo_10x_conta_desde_a_entrada_do_ajudante_promovido() {
        let inicio = Instant::now();
        let mut quadro = quadro(vec![1; 3], 3, 0);
        anunciar(&mut quadro, 1, &[0, 1]);
        anunciar(&mut quadro, 2, &[0]);
        anunciar(&mut quadro, 3, &[0, 2]);
        quadro.pedir(1, inicio).unwrap();
        quadro.pedir(1, inicio).unwrap();
        quadro
            .concluir(1, 1, true, inicio + Duration::from_secs(1))
            .unwrap();
        quadro.pedir(3, inicio + Duration::from_secs(2)).unwrap();

        let entrada = inicio + Duration::from_secs(3);
        assert!(quadro.pedir(2, entrada).unwrap().modo_final);
        quadro.saiu(1).unwrap();
        assert_eq!(
            quadro.estado(0),
            Some(&EstadoPeca::EmVoo {
                dono: 2,
                inicio: entrada,
                ajudantes: vec![]
            })
        );

        quadro.soltar(3, 2).unwrap();
        let limite = entrada + Duration::from_secs(10);
        assert_eq!(quadro.pedir(3, limite).unwrap().peca, 2);
        quadro.soltar(3, 2).unwrap();
        let roubada = quadro.pedir(3, limite + Duration::from_millis(1)).unwrap();
        assert_eq!((roubada.peca, roubada.roubada_de), (0, Some(2)));
    }

    #[test]
    fn f14a1_fonte_unica_prioriza_outra_peca_e_volta_a_atrasada() {
        let agora = Instant::now();
        let mut quadro = quadro(vec![1; 2], 1, 0);
        anunciar(&mut quadro, 1, &[0]);
        assert_eq!(quadro.pedir(1, agora).unwrap().peca, 0);
        quadro.have(1, 1).unwrap();
        quadro.soltar_por_atraso(1, 0).unwrap();
        assert_eq!(quadro.pedir(1, agora).unwrap().peca, 1);
        assert_eq!(
            quadro.pedir(1, agora),
            None,
            "a peça atrasada excede o orçamento"
        );
        quadro.concluir(1, 1, true, agora).unwrap();
        assert_eq!(quadro.pedir(1, agora).map(|pedido| pedido.peca), Some(0));
    }

    #[test]
    fn f14a1_atrasada_so_volta_quando_a_alternativa_sai() {
        let agora = Instant::now();
        let mut quadro = quadro(vec![1], 1, 0);
        anunciar(&mut quadro, 1, &[0]);
        anunciar(&mut quadro, 2, &[0]);
        assert_eq!(quadro.pedir(1, agora).unwrap().peca, 0);
        quadro.soltar_por_atraso(1, 0).unwrap();
        assert_eq!(quadro.pedir(1, agora), None);
        quadro.saiu(2).unwrap();
        assert_eq!(quadro.pedir(1, agora).map(|pedido| pedido.peca), Some(0));
    }

    #[test]
    fn f14a1_todas_as_fontes_atrasadas_podem_pedir_quando_na_fila() {
        let agora = Instant::now();
        let mut quadro = quadro(vec![1], 2, 0);
        for id in [1, 2] {
            anunciar(&mut quadro, id, &[0]);
            assert_eq!(quadro.pedir(id, agora).unwrap().peca, 0);
            quadro.soltar_por_atraso(id, 0).unwrap();
        }
        for (id, outro) in [(1, 2), (2, 1)] {
            assert_eq!(quadro.pedir(id, agora).map(|pedido| pedido.peca), Some(0));
            assert_eq!(quadro.pedir(outro, agora), None, "a peça está em voo");
            quadro.soltar_por_atraso(id, 0).unwrap();
        }
        assert_eq!(quadro.pecas[0].atrasados.len(), 2);
    }

    proptest! {
        #![proptest_config(ProptestConfig { failure_persistence: None, ..ProptestConfig::default() })]

        #[test]
        fn invariantes_apos_cada_operacao(
            n in 1..=64usize,
            aparelhos in 1..=8usize,
            orcamento in 0..=100u64,
            operacoes in prop::collection::vec((0..10u8, any::<u8>(), any::<u64>()), 1..=200),
        ) {
            let tamanhos: Vec<_> = (1..=n as u64).collect();
            let mut quadro = quadro(tamanhos.clone(), orcamento, 7);
            let mut conectados = vec![None; aparelhos];
            let mut concluidas = HashSet::new();
            let mut atrasos = HashSet::new();
            let mut agora = Instant::now();
            let mut tem_anterior = 0;

            for (operacao, aparelho, dado) in operacoes {
                let slot = aparelho as usize % aparelhos;
                let id = slot as AparelhoId;
                let peca = (dado % n as u64) as usize;
                match operacao {
                    0 => {
                        let resultado = quadro.entrou(id);
                        prop_assert_eq!(resultado.is_ok(), conectados[slot].is_none());
                        if resultado.is_ok() {
                            conectados[slot] = Some((false, vec![false; n]));
                        }
                    }
                    1 => {
                        if let Some((anunciou, tem)) = &mut conectados[slot] && !*anunciou {
                            let mut bits = vec![0; n.div_ceil(8)];
                            for (i, tem) in tem.iter_mut().enumerate() {
                                *tem = dado & (1 << i) != 0;
                                bits[i / 8] |= u8::from(*tem) << (7 - i % 8);
                            }
                            prop_assert!(quadro.bitfield(id, &bits).is_ok());
                            *anunciou = true;
                        }
                    }
                    2 => {
                        let resultado = quadro.have(id, peca);
                        prop_assert_eq!(resultado.is_ok(), conectados[slot].is_some());
                        if let Some((anunciou, tem)) = &mut conectados[slot] {
                            tem[peca] = true;
                            *anunciou = true;
                        }
                    }
                    3 => {
                        if let Some(atribuicao) = quadro.pedir(id, agora) {
                            prop_assert!(atribuicao.peca < n, "I6: índice fora do job");
                            prop_assert!(!concluidas.contains(&atribuicao.peca), "I6: peça Tem atribuída");
                            prop_assert!(conectados[slot].as_ref().is_some_and(|(_, tem)| tem[atribuicao.peca]), "I6: peça que o aparelho não tem");
                        }
                    }
                    4..=6 | 9 => {
                        let atribuicoes: Vec<_> = (0..n).filter(|&i| {
                            matches!(quadro.estado(i), Some(EstadoPeca::EmVoo { dono, ajudantes, .. })
                                if *dono == id || ajudantes.iter().any(|&(ajudante, _)| ajudante == id))
                        }).collect();
                        let alvo = if atribuicoes.is_empty() { peca } else { atribuicoes[peca % atribuicoes.len()] };
                        let resultado = match operacao {
                            6 => quadro.soltar(id, alvo),
                            9 => quadro.soltar_por_atraso(id, alvo),
                            _ => quadro.concluir(id, alvo, operacao == 4, agora).map(|_| ()),
                        };
                        prop_assert_eq!(resultado.is_ok(), !atribuicoes.is_empty());
                        if operacao == 4 && resultado.is_ok() {
                            concluidas.insert(alvo);
                            atrasos.retain(|&(_, i)| i != alvo);
                        }
                        if operacao == 9 && resultado.is_ok() {
                            atrasos.insert((slot, alvo));
                        }
                    }
                    7 => {
                        prop_assert_eq!(quadro.saiu(id).is_ok(), conectados[slot].take().is_some());
                        atrasos.retain(|&(fonte, _)| fonte != slot);
                    }
                    _ => agora += Duration::from_secs(dado % 31),
                }

                let (mut tem, mut em_voo, mut na_fila) = (0, 0, 0);
                let (mut copias, mut bytes) = (0, 0);
                for (i, &tamanho) in tamanhos.iter().enumerate() {
                    match quadro.estado(i) {
                        Some(EstadoPeca::Tem) => tem += 1,
                        Some(EstadoPeca::NaFila) => na_fila += 1,
                        Some(EstadoPeca::EmVoo { dono, ajudantes, .. }) => {
                            em_voo += 1;
                            let mut unicos = HashSet::new();
                            for &id in std::iter::once(dono).chain(ajudantes.iter().map(|(id, _)| id)) {
                                prop_assert!(unicos.insert(id), "I2: aparelho repetido na peça");
                                prop_assert!(conectados.get(id as usize).and_then(Option::as_ref).is_some_and(|(_, tem)| tem[i]), "I2: titular desconectado ou sem a peça");
                                copias += 1;
                                bytes += tamanho;
                            }
                        }
                        None => prop_assert!(false, "I1: peça sem estado"),
                    }
                    let disponibilidade = conectados.iter().flatten().filter(|(_, tem)| tem[i]).count();
                    prop_assert_eq!(quadro.disponibilidade(i), Some(disponibilidade), "I4: disponibilidade da peça {}", i);
                    prop_assert_eq!(quadro.estado(i) == Some(&EstadoPeca::Tem), concluidas.contains(&i), "I5: estado Tem da peça {}", i);
                    let fontes = conectados.iter().enumerate()
                        .filter(|(_, reg)| reg.as_ref().is_some_and(|(_, tem)| tem[i]))
                        .map(|(fonte, _)| fonte);
                    let fontes_sem_atraso = fontes.clone().filter(|&fonte| !atrasos.contains(&(fonte, i))).count();
                    for fonte in fontes {
                        prop_assert_eq!(
                            quadro.bloqueado(i, fonte as AparelhoId),
                            atrasos.contains(&(fonte, i)) && fontes_sem_atraso > 0,
                            "I7: bloqueio da peça {} para o aparelho {}", i, fonte
                        );
                    }
                }
                prop_assert_eq!(tem + em_voo + na_fila, n, "I1: total de peças");
                prop_assert_eq!(quadro.estado(n), None, "I1: peça fora do job");
                prop_assert_eq!(quadro.completo(), tem == n, "I1: estado do job");
                prop_assert_eq!(quadro.bytes_em_voo(), bytes, "I3: bytes em voo");
                prop_assert!(bytes <= orcamento || copias == 1, "I3: orçamento excedido");
                prop_assert!(tem >= tem_anterior, "I5: menos peças Tem");
                tem_anterior = tem;
            }
        }
    }
}
