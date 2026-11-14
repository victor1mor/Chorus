use anyhow::{Result, ensure};
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use std::net::IpAddr;
use std::path::{Component, Path, PathBuf};
use url::{Host, Url};

pub fn saida_segura(raiz: &Path, pedido: &str) -> Result<PathBuf> {
    let caminho = Path::new(pedido);
    // Path::components normaliza pontos internos e só reconhece separadores da plataforma.
    ensure!(
        !pedido.is_empty()
            && !pedido.starts_with(['/', '\\'])
            && !pedido.contains([':', '\0'])
            && !pedido
                .split(['/', '\\'])
                .any(|parte| matches!(parte, "." | ".."))
            && caminho
                .components()
                .all(|parte| matches!(parte, Component::Normal(_))),
        "output must be a relative path inside the download directory"
    );
    Ok(raiz.join(caminho))
}

pub fn ip_proibido(ip: IpAddr) -> bool {
    match ip.to_canonical() {
        IpAddr::V4(ip) => {
            let [primeiro, segundo, ..] = ip.octets();
            ip.is_loopback()
                || ip.is_private()
                || ip.is_link_local()
                || ip.is_unspecified()
                || ip.is_multicast()
                || ip.is_broadcast()
                || (primeiro == 100 && (64..=127).contains(&segundo))
        }
        IpAddr::V6(ip) => {
            ip.is_loopback()
                || ip.is_unicast_link_local()
                || ip.is_unique_local()
                || ip.is_unspecified()
                || ip.is_multicast()
        }
    }
}

pub fn host_proibido(url: &Url) -> bool {
    match url.host() {
        Some(Host::Ipv4(ip)) => ip_proibido(ip.into()),
        Some(Host::Ipv6(ip)) => ip_proibido(ip.into()),
        _ => false,
    }
}

pub fn validar_url(pedido: &str, rede_protegida: bool) -> Result<()> {
    let url = Url::parse(pedido)?;
    ensure!(
        matches!(url.scheme(), "http" | "https"),
        "unsupported URL scheme: {}",
        url.scheme()
    );
    ensure!(
        !rede_protegida || !host_proibido(&url),
        "destination is a private or local address"
    );
    Ok(())
}

pub struct ResolvedorProtegido;

impl Resolve for ResolvedorProtegido {
    fn resolve(&self, name: Name) -> Resolving {
        Box::pin(async move {
            let addrs: Vec<_> = tokio::net::lookup_host((name.as_str(), 0)).await?.collect();
            if addrs.iter().any(|addr| ip_proibido(addr.ip())) {
                return Err("destination resolves to a private or local address".into());
            }
            Ok(Box::new(addrs.into_iter()) as Addrs)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifica_ips() {
        for (ip, proibido) in [
            ("127.0.0.1", true),
            ("10.0.0.1", true),
            ("172.16.0.1", true),
            ("192.168.1.1", true),
            ("169.254.1.1", true),
            ("100.64.0.1", true),
            ("100.127.255.255", true),
            ("0.0.0.0", true),
            ("224.0.0.1", true),
            ("255.255.255.255", true),
            ("::", true),
            ("::1", true),
            ("fe80::1", true),
            ("fc00::1", true),
            ("ff02::1", true),
            ("::ffff:127.0.0.1", true),
            ("::ffff:8.8.8.8", false),
            ("100.63.255.255", false),
            ("100.128.0.0", false),
            ("8.8.8.8", false),
            ("1.1.1.1", false),
            ("2606:4700::1111", false),
        ] {
            assert_eq!(ip_proibido(ip.parse().unwrap()), proibido, "{ip}");
        }
    }

    #[test]
    fn valida_caminhos_relativos() {
        let raiz = Path::new("downloads");
        for pedido in [
            "",
            "/abs",
            r"\abs",
            r"C:\x",
            r"\\srv\share",
            "..",
            "a/../b",
            r"a\..\b",
            ".",
            "a/./b",
            "a:b",
            "a\0b",
        ] {
            assert!(saida_segura(raiz, pedido).is_err(), "{pedido:?}");
        }
        for pedido in ["arquivo.bin", "sub/arquivo.bin"] {
            assert_eq!(saida_segura(raiz, pedido).unwrap(), raiz.join(pedido));
        }
    }

    #[tokio::test]
    async fn recusa_nome_que_resolve_para_loopback() {
        let resultado = ResolvedorProtegido
            .resolve("localhost".parse().unwrap())
            .await;
        let erro = resultado.err().expect("localhost deve ser bloqueado");
        assert_eq!(
            erro.to_string(),
            "destination resolves to a private or local address"
        );
    }
}
