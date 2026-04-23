# Mecanismos de Coordinación

El desarrollo del trabajo fue realizado en Python.

## Arquitectura de Sincronización (EOF)

El principal desafío del sistema es la propagación de la señal de finalización o EOF (End Of File). En una arquitectura de Work Queues, el mensaje de EOF enviado por el Gateway es consumido por una única instancia de SumFilter, dejando a las demás en una espera infinita.

Para resolver este problema, se implementó:

1. Broadcast de Control: Cuando una instancia de SumFilter recibe el EOF original del Gateway, utiliza un Exchange de tipo Fanout (SUM_CONTROL_EXCHANGE) para retransmitir la señal a todos sus pares.

2. Sincronización por Marcadores: Cada nodo, al recibir la señal del canal de control, inyecta un mensaje especial denominado CONTROL_EOF_MARKER en su propia cola de entrada. Esto permite que el flujo de cierre se unifique con el flujo de procesamiento de datos, asegurando que no se cierren las conexiones hasta haber procesado el último mensaje de la cola.

<br>
<img src="./diagrams/pipeline.drawio.svg">

## Justificación del uso de Threads

El uso de hilos en el SumFilter es una decisión técnica crítica motivada por las restricciones de la librería de conectividad (Pika) y la necesidad de concurrencia en el monitoreo:

- Monitoreo Multicanal: El filtro debe estar a la escucha de dos fuentes de mensajes simultáneamente: la cola de datos (proveniente del Gateway) y el exchange de control (proveniente de sus pares). Como el consumo de RabbitMQ es bloqueante (start_consuming), se requiere un hilo dedicado para el canal de control.

- Thread-Safety y Separación de Responsabilidades: La librería Pika no es thread-safe. Intentar enviar datos desde un hilo mientras otro hilo consume sobre la misma conexión provoca errores de protocolo.

## Escalabilidad y Sharding

Para permitir que el sistema escale horizontalmente sin perder la consistencia del conteo:

- Sharding por Hash: El SumFilter aplica una función de hash MD5 sobre el nombre de la fruta para determinar el destino (hash % AGGREGATION_AMOUNT). Se eligió MD5 por ser computacionalmente eficiente y garantizar **determinismo**: la misma fruta siempre mapea al mismo agregador, asegurando la consistencia del conteo distribuido.

- Named Queues (Colas con Nombre): Se modificó el middleware para utilizar colas con nombre fijo en los nodos con identidad (aggregation_0, aggregation_1, etc.). Esto soluciona la condición de carrera en el arranque, permitiendo que los mensajes esperen en el broker si el emisor es más rápido que el receptor en iniciar.

## Tolerancia a Fallos y Robustez

Se incorporó en el middleware un mecanismo de QoS con un prefetch_count=1. Esto asegura que:

1. Los mensajes se distribuyan equitativamente entre los nodos disponibles.

2. Si un nodo falla durante el procesamiento, el mensaje no confirmado (ack) regrese a la cola para ser procesado por otra instancia, evitando la pérdida de información crítica.
