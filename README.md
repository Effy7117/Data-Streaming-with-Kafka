# Data-Streaming-with-Kafka

The project aimed to develop a system capable of live-streaming weather data for Winnipeg and Vancouver while storing this information in a PostgreSQL database. By designing Python scripts for Kafka producer and consumer, along with auxiliary functions for data generation and database interaction, the project demonstrated exemplary execution and adherence to requirements.

Throughout the project, Zookeeper and Kafka services were seamlessly initiated and maintained, ensuring uninterrupted operation of the data streaming pipeline. The Python code for both producer and consumer components was well-structured, thoroughly commented, and exhibited robust functionality. The Kafka producer efficiently generated synthetic weather data at five-second intervals, adhering to predefined ranges for temperature, wind speed, and humidity. This data was successfully written to the designated Kafka topic, facilitating smooth data ingestion into the pipeline.

On the consumer side, the script subscribed to the Kafka topic, retrieved weather data messages, appended timestamps, and persisted the information into the PostgreSQL database. 

In conclusion, the project exemplified the effective utilization of Apache Kafka for building a robust streaming data pipeline. The seamless integration of Kafka with PostgreSQL, coupled with well-organized Python code, underscore the reliability, scalability, and potential for future enhancements. Overall, the project achieved its objectives.
