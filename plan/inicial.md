Sim. Se por “backend do Celery” você quer dizer usar o PGMQ como broker/fila de tasks, então a integração certa é via Kombu Transport.

Pelo que encontrei, não existe hoje um transport PGMQ oficial/pronto para Celery/Kombu. O Celery 5.6 lista RabbitMQ, Redis, SQS, Kafka, Google Pub/Sub etc., mas não PGMQ. Também não encontrei um projeto público relevante celery-pgmq / kombu-pgmq nas buscas por GitHub/PyPI.

O PGMQ, por outro lado, encaixa muito bem conceitualmente: ele já fornece queue persistente dentro do Postgres, visibility timeout, leitura, delete/archive e sem daemon adicional.

A arquitetura ficaria aproximadamente assim:

Celery
  │
  │ producer / consumer API
  ▼
Kombu
  │
  │ custom transport
  ▼
kombu_pgmq.Transport
  │
  ├── _put()       -> pgmq.send(...)
  ├── _get()       -> pgmq.read(...)
  ├── ack()        -> pgmq.delete(...)
  ├── reject()     -> visibility timeout / delete
  └── queues       -> pgmq queues
  │
  ▼
PostgreSQL
  └── pgmq extension

Kombu existe justamente para abstrair esse layer: ele suporta transports plugáveis e permite criar um transport próprio.

A parte interessante é que PGMQ parece quase feito sob medida para um transport estilo SQS. Eu começaria olhando muito mais para kombu.transport.SQS do que para Redis/RabbitMQ, porque a semântica é parecida:

PGMQ                         SQS / Celery
------------------------------------------------
pgmq.send()                  publish
pgmq.read(vt=...)            receive + visibility timeout
pgmq.delete(msg_id)          ack
visibility timeout expires   message redelivery
queue name                   Celery queue

Então um MVP do transport provavelmente seria relativamente pequeno.

Algo conceitualmente parecido com:

from kombu.transport import virtual


class Channel(virtual.Channel):

    def _new_queue(self, queue, **kwargs):
        self.pgmq.create(queue)

    def _put(self, queue, message, **kwargs):
        self.pgmq.send(
            queue,
            message,
        )

    def _get(self, queue, timeout=None):
        messages = self.pgmq.read(
            queue,
            vt=self.visibility_timeout,
            qty=1,
        )

        if not messages:
            raise self.Empty()

        msg = messages[0]

        # guardar msg_id para o ack posterior
        message = msg.message
        message["_pgmq_msg_id"] = msg.msg_id

        return message


class Transport(virtual.Transport):
    Channel = Channel

    driver_type = "pgmq"
    driver_name = "pgmq"

    default_port = 5432

Mas existe uma sutileza importante: não delete a mensagem no _get().

Você precisa manter:

read
 ↓
PGMQ torna invisível durante VT
 ↓
Celery executa task
 ↓
ACK
 ↓
pgmq.delete(msg_id)

Se o worker morrer:

read
 ↓
worker crash
 ↓
sem delete
 ↓
visibility timeout expira
 ↓
mensagem aparece novamente

Isso dá ao Celery a semântica de redelivery que você quer.

Eu desenharia o pacote como algo separado:

kombu-pgmq/
├── pyproject.toml
└── kombu_pgmq/
    ├── __init__.py
    └── transport.py

E permitiria:

app = Celery(
    "myapp",
    broker="pgmq://user:pass@localhost:5432/mydb",
)

ou, inicialmente, sem registrar schema:

app.conf.broker_transport = "kombu_pgmq.transport:Transport"
app.conf.broker_url = "postgresql://..."

Uma decisão que eu tomaria logo de início é não implementar toda a semântica AMQP. O virtual.Transport do Kombu existe exatamente para transports que não são AMQP nativos. Seu primeiro release poderia suportar somente:

✓ task queues
✓ routing_key -> queue
✓ ack
✓ reject
✓ redelivery
✓ visibility_timeout
✓ ETA/countdown (Celery pode cuidar de parte disso)

depois:
○ priority
○ fanout
○ topic exchange
○ remote control
○ celery events

Aliás, PGMQ hoje já anuncia FIFO, message groups e topic-based routing com wildcards, então existe espaço para mapear mais recursos do Kombu depois.

Tem uma segunda possibilidade: se por “backend” você quis dizer literalmente o Celery Result Backend (AsyncResult, guardar SUCCESS, resultado da task etc.), aí não é Kombu. Seria uma implementação de celery.backends.base.BaseBackend. Mas eu não usaria PGMQ para isso: resultado é estado persistente/key-value e fila não é uma abstração particularmente boa para esse problema. Você pode perfeitamente fazer:

app = Celery(
    broker="pgmq://...",
    backend="db+postgresql://...",
)

Ou seja:

Tasks:
Celery -> Kombu -> PGMQ -> PostgreSQL

Results:
Celery -> SQLAlchemy backend -> PostgreSQL

Assim você termina com um único PostgreSQL, sem Redis/RabbitMQ, que imagino ser justamente o atrativo dessa ideia.

Se eu fosse implementar isso, faria um kombu-pgmq e não modificaria Celery diretamente. Essa separação também torna o projeto potencialmente publicável upstream/como pacote PyPI. O PGMQ inclusive já tem client Python oficial, embora eu consideraria usar psycopg diretamente para manter o transport pequeno e controlar pooling/bloqueio.

E tem algumas decisões importantes de implementação — especialmente como implementar drain_events(), ACK bookkeeping, polling eficiente e LISTEN/NOTIFY para evitar polling no Postgres. Aí dá para fazer um transport bem elegante, não apenas um adapter funcionando.

Eu tenho q criar dois repos/ pacotes 

Um
Para o kombu e outro para o celery? 

Não. Para usar PGMQ como broker do Celery, eu faria um único pacote/repo, por exemplo kombu-pgmq.

O Celery já usa o Kombu internamente. Então o fluxo fica:

Celery
  ↓
Kombu
  ↓
kombu-pgmq
  ↓
PGMQ / PostgreSQL

Seu pacote implementaria algo como kombu.transport.virtual.Transport + Channel, e o Celery passaria a usar esse transport através do Kombu.

Você só precisaria de um segundo pacote específico para Celery se quisesse adicionar alguma coisa que não pertence ao transport, por exemplo um result backend próprio:

kombu-pgmq
  → broker / filas / publish / consume / ack

celery-pgmq
  → result backend, AsyncResult, estados SUCCESS/FAILURE etc.

Mas eu não começaria assim. Para o objetivo de substituir Redis/RabbitMQ pelo PGMQ como broker, um repo só é a arquitetura mais limpa.

Inclusive eu estruturaria algo nessa linha:

kombu-pgmq/
├── pyproject.toml
├── src/
│   └── kombu_pgmq/
│       ├── __init__.py
│       ├── transport.py
│       └── connection.py
└── tests/
    ├── test_transport.py
    └── test_celery_integration.py

E os testes de integração podem subir um Celery real sem você precisar criar um pacote celery-pgmq.

Se quiser, eu posso te desenhar a API mínima que o Transport e Channel precisam implementar para o Celery realmente funcionar, incluindo ack, reject, drain_events e visibility timeout.