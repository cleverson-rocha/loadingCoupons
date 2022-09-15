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

## Study suggestion

https://nodejs.org/api/stream.html#organization-of-this-document
