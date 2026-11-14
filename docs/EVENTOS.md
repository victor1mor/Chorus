# Eventos do Chorus v1

Com `--events`, stdout contém exclusivamente eventos, um objeto JSON por linha
(NDJSON). As mensagens de texto vão para stderr. O WebSocket envia os mesmos
objetos JSON em mensagens de texto.

Jobs BitTorrent emitem `Progress` e `StateChanged` no mesmo contrato v1.

| Campo | Tipo | Significado |
| --- | --- | --- |
| `v` | inteiro | Versão do contrato, atualmente `1`. |
| `seq` | inteiro sem sinal de 64 bits | Número do evento no processo, começando em `1`. Pode haver lacunas. |
| `type` | string | `JobCreated`, `StateChanged` ou `Progress`. |
| `data` | objeto | Campos específicos do tipo. `id` é sempre o identificador do job, uma string. |

### JobCreated

O job foi criado e registrado.

```json
{"v":1,"seq":1,"type":"JobCreated","data":{"id":"job-123"}}
```

### StateChanged

`from` e `to` são os estados anterior e novo. Os estados simples são strings
(`Waiting`, `Active`, `Seeding`, `Paused`, `Cancelled`, `Complete`). Uma falha
é um objeto `{"Failed":"categoria"}`. As categorias são `InvalidInput`,
`Network`, `Integrity`, `Storage` e `Internal`.

```json
{"v":1,"seq":2,"type":"StateChanged","data":{"id":"job-123","from":"Waiting","to":"Active"}}
{"v":1,"seq":5,"type":"StateChanged","data":{"id":"job-123","from":"Active","to":"Complete"}}
{"v":1,"seq":6,"type":"StateChanged","data":{"id":"job-456","from":"Active","to":{"Failed":"Network"}}}
```

### Progress

`bytes_downloaded` é o número de bytes baixados, incluindo os aproveitados na
retomada. `bytes_total` é o tamanho total usado para o progresso, ou `null`
para um download em fluxo. Os valores numéricos são inteiros sem sinal de 64
bits. No stdout, sai no máximo um `Progress` a cada 250 ms por job. Esse
limite pode suprimir o último `Progress`. O desfecho do job é o `StateChanged`
com `to` igual a `"Complete"` ou a um objeto como `{"Failed":"Network"}`.

```json
{"v":1,"seq":3,"type":"Progress","data":{"id":"job-123","bytes_downloaded":1024,"bytes_total":4096}}
```

As URLs exibidas nas mensagens têm usuário, senha, query e fragmento removidos.
O token de autenticação RPC fica fora da saída.

Campos novos podem ser adicionados sem alterar `v`. Uma mudança incompatível
exige `v: 2`.
