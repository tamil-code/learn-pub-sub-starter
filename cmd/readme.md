1. Start the server -> creates a queue with the name game_logs and bind it to the peril_topic exchange and on pause publishes a message to the peril_direct exchange with the key pause
2. Start the client -> creates a queue with the username and bind it to the peril_direct exchange (direct exchange with key pause)
