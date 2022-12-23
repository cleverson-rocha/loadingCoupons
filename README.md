# Carregar cupons na base

## Proposta

- _bonuz.prizes_

  > Capturar os prizes ativos, com delivery engine coupon e os prizes carrefour marca própria.

- _bonuzCoupon.coupons_

  > Carregar nessa collection o prize capturado e associar a um cupom gerado. Se for carregado 1000 cupons, serão 1000 documentos do mesmo prize com seu respectivo cupom.

- _bonuzCoupon.batches_
  > Essa collection recebe o total de cupons vinculados a um único prize. Se for carregado 1000 cupons, será exibido um único documento com o total de cupons vinculados ao prize.

## Conectar no banco utilizando o mongoClient

**Connection**

1. Validar a conexão com o banco

**Database.Collection**

- _bonuz.prizes_
- _bonuzCoupon.coupons_
- _bonuzCoupon.batches_

## Create documents

2. Ler collection _bonuz.prizes_ e montar um array com os prizes ACTIVE, deliveryEngine COUPON e em especial, os prizes marca própria Carrefour (deliveryEngine XDH).
3. Criar um gerador de cupons randômico.
4. Fazer **upsert** nas collections _bonuzCoupon.coupons_ e _bonuzCoupon.batches_.

## Evoluções

1. Criar uma validação para que a aplicação só carregue cupons em ofertas com um mínimo de 3 meses para vencimento da base.

## Stream

https://nodejs.org/docs/latest-v14.x/api/stream.html

- _Writable chunks callback_

  https://nodejs.org/docs/latest-v14.x/api/stream.html#stream_writable_writev_chunks_callback

- _mobile-number-migration_

  https://github.com/Minutrade/mobile-number-migration/tree/da78bbad6005dfa4c82ab28f02762928d1c2027c

## Pendências

- Memory leak

  - Da documentação do Node: One important caveat is that if the Readable stream emits an error during processing, the Writable destination is not closed automatically. If an error occurs, it will be necessary to manually close each stream in order to prevent memory leaks.

- Liberar terminal

- Melhorar a performance para grandes quantidades de coupons por batchs