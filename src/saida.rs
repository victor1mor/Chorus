use crate::job::redact_source;
use std::fmt::Display;
use std::sync::atomic::{AtomicBool, Ordering};
use url::Url;

static MODO_EVENTOS: AtomicBool = AtomicBool::new(false);

pub fn ativar_modo_eventos() {
    MODO_EVENTOS.store(true, Ordering::Relaxed);
}

pub fn modo_eventos() -> bool {
    MODO_EVENTOS.load(Ordering::Relaxed)
}

fn tem_segredo(url: &Url) -> bool {
    !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
}

pub fn redigir_url(trecho: &str) -> std::borrow::Cow<'_, str> {
    if let Ok(url) = Url::parse(trecho)
        && tem_segredo(&url)
    {
        redact_source(trecho).into()
    } else {
        trecho.into()
    }
}

pub fn redigir_erro_http(mut erro: anyhow::Error) -> anyhow::Error {
    if let Some(url) = erro
        .downcast_mut::<reqwest::Error>()
        .and_then(reqwest::Error::url_mut)
        && tem_segredo(url)
    {
        // O erro já carrega a URL inteira, inclusive delimitadores como os de um host IPv6.
        *url = Url::parse(&redact_source(url.as_str())).expect("a URL redigida continua válida");
    }
    erro
}

pub fn redigir_urls(texto: &str) -> String {
    let separador = |c: char| c.is_whitespace() || "()<>\"',[]".contains(c);
    let mut saida = String::with_capacity(texto.len());
    let mut inicio = 0;
    for (fim, delim) in texto.match_indices(separador).chain([(texto.len(), "")]) {
        let trecho = &texto[inicio..fim];
        if trecho.contains("://") {
            saida.push_str(&redigir_url(trecho));
        } else {
            saida.push_str(trecho);
        }
        saida.push_str(delim);
        inicio = fim + delim.len();
    }
    saida
}

pub fn info(msg: impl Display) {
    if modo_eventos() {
        erro(msg);
    } else {
        println!("{}", redigir_urls(&msg.to_string()));
    }
}

pub fn erro(msg: impl Display) {
    eprintln!("{}", redigir_urls(&msg.to_string()));
}

#[cfg(test)]
mod tests {
    use super::redigir_urls;

    #[test]
    fn redige_url_em_erro() {
        let saida =
            redigir_urls("error sending request for url (https://user:senha@host/x?token=abc)");
        assert!(!saida.contains("senha"));
        assert!(!saida.contains("abc"));
        assert!(saida.contains("https://host/x"));
    }

    #[test]
    fn preserva_texto_sem_url() {
        let texto = "Chorus: arquivo [1], pronto.\n";
        assert_eq!(redigir_urls(texto), texto);
    }

    #[test]
    fn redige_duas_urls_e_preserva_delimitadores() {
        assert_eq!(
            redigir_urls("['https://user@host/x#parte', <https://host/y?token=abc>]"),
            "['https://host/x', <https://host/y>]"
        );
    }
}
