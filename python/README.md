# Informe - Trabajo práctico de Coordinación

## Coordinación entre instancias de Sum

Cada instancia de Sum es un contenedor independiente que lee de una cola compartida (`input_queue`). RabbitMQ distribuye los mensajes de forma competitiva: cada mensaje lo procesa exactamente una instancia. Cada instancia acumula en memoria los pares `(fruta, cantidad)` agrupados por `client_id`.

El problema de coordinación surge con el EOF: el mensaje de fin de ingesta de un cliente llega a una sola instancia, pero las demás también tienen datos acumulados de ese cliente que deben enviarse.

Para resolverlo se usa un **exchange de control** de tipo direct (`SUM_CONTROL_EXCHANGE`). Cuando una instancia recibe el EOF de un cliente desde `input_queue`, no flushea inmediatamente sino que publica ese EOF en el exchange de control. Todas las instancias están suscriptas con su propia routing key (`sum_0`, `sum_1`, ...), por lo que cada una recibe una copia. Recién al recibirlo, cada instancia flushea sus datos acumulados hacia los Aggregators —particionados por `hash(fruta, client_id) % M`— y les envía su propio EOF.

De esta forma, con `N` instancias de Sum, cada Aggregator recibe exactamente `N` EOFs por cliente, lo que le permite saber cuándo consolidar su top parcial.

### Modelo de threads

Dado que `pika` en modo blocking no permite escuchar dos fuentes simultáneamente desde un mismo thread, cada instancia de Sum corre dos threads:

- **Thread principal**: consume `input_queue`. Al recibir datos los acumula; al recibir un EOF lo reenvía al exchange de control.
- **Thread secundario**: consume la routing key propia en `SUM_CONTROL_EXCHANGE`. Al recibir el EOF broadcasteado ejecuta el flush hacia los Aggregators.

Ambos threads comparten `amount_by_fruit`, protegido con un `Lock` para evitar race conditions entre la escritura del thread principal y el `pop` del secundario al momento del flush.

### Shutdown

Al recibir `SIGTERM`, el proceso detiene el consumo del thread principal. Una vez que `start_consuming` retorna, el main thread detiene el thread secundario y espera su finalización con `join` antes de cerrar las conexiones.