# Carregar cupons na base

## Conectar no banco utilizando o mongoClient

**Connection**

1. Validar a conexão com o banco

**Database.Collection**

- _bonuz.prizes_
- _bonuzCoupon.coupons_
- _bonuzCoupon.batches_

## Create documents

2. Ler collection _bonuz.prizes_ e montar um array com os prizes ACTIVES, com COUPONS e em especial, os prizes Carrefour (XDH).
3. Criar um gerador de cupons randomico.
4. Fazer **upsert** nas collections _bonuzCoupon.coupons_ e _bonuzCoupon.batches_.

## Study suggestion

https://nodejs.org/api/stream.html#organization-of-this-document
