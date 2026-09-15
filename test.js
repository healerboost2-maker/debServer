const http = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 10000;

// ============================================================
// SERVER CONFIGURATION
// ============================================================

const SERVER_NAME = "AudioBridge";
const SERVER_VERSION = "3.0.0";

const REGISTRATION_TIMEOUT_MS = 10000;
const HEARTBEAT_INTERVAL_MS = 30000;

// Prevent a slow receiver from accumulating unlimited audio.
const MAX_RECEIVER_BUFFERED_BYTES = 2 * 1024 * 1024;

// ============================================================
// ROOMS
// ============================================================

const rooms = {
    am: {
        name: "AM",

        transmitters: new Set(),
        receivers: new Set(),

        config: {
            sampleRate: 44100,
            channels: 2
        }
    },

    fm: {
        name: "FM",

        transmitters: new Set(),
        receivers: new Set(),

        config: {
            sampleRate: 44100,
            channels: 2
        }
    }
};

// ============================================================
// HELPERS
// ============================================================

function sendJson(ws, payload) {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        return false;
    }

    try {
        ws.send(JSON.stringify(payload));
        return true;
    } catch (error) {
        console.error("[WS JSON SEND ERROR]", error.message);
        return false;
    }
}


function getRoomFromPath(pathname) {
    if (
        pathname === "/amtx" ||
        pathname === "/amrx"
    ) {
        return "am";
    }

    if (
        pathname === "/fmtx" ||
        pathname === "/fmrx"
    ) {
        return "fm";
    }

    return null;
}


function isTransmitterPath(pathname) {
    return (
        pathname === "/amtx" ||
        pathname === "/fmtx"
    );
}


function isReceiverPath(pathname) {
    return (
        pathname === "/amrx" ||
        pathname === "/fmrx"
    );
}


function removeFromRoom(ws) {
    if (!ws || !ws.stationRoom) {
        return;
    }

    const room = rooms[ws.stationRoom];

    if (!room) {
        return;
    }

    room.transmitters.delete(ws);
    room.receivers.delete(ws);
}


function closeSocket(ws, code, reason) {
    try {
        if (ws.readyState === WebSocket.OPEN) {
            ws.close(code, reason);
        } else if (ws.readyState === WebSocket.CONNECTING) {
            ws.terminate();
        }
    } catch (error) {
        try {
            ws.terminate();
        } catch (_) {
            // Ignore.
        }
    }
}


function getActiveCasterId(room) {
    for (const tx of room.transmitters) {
        if (
            tx.registered &&
            tx.casterId
        ) {
            return tx.casterId;
        }
    }

    return null;
}


// ============================================================
// HTTP SERVER
// ============================================================

const server = http.createServer((req, res) => {

    let url;

    try {
        url = new URL(
            req.url,
            `http://${req.headers.host || "localhost"}`
        );
    } catch (error) {

        res.writeHead(400, {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
        });

        res.end(JSON.stringify({
            status: "error",
            error: "Invalid URL"
        }));

        return;
    }


    // --------------------------------------------------------
    // HEALTH
    // --------------------------------------------------------

    if (url.pathname === "/health") {

        res.writeHead(200, {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
        });

        res.end(JSON.stringify({
            status: "ok",
            service: SERVER_NAME,
            version: SERVER_VERSION,
            websocket: true,

            endpoints: {
                amtx: "/amtx",
                amrx: "/amrx",
                fmtx: "/fmtx",
                fmrx: "/fmrx"
            },

            timestamp: new Date().toISOString()
        }));

        return;
    }


    // --------------------------------------------------------
    // STATUS
    // --------------------------------------------------------

    if (url.pathname === "/status") {

        res.writeHead(200, {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
        });

        res.end(JSON.stringify({

            service: SERVER_NAME,
            version: SERVER_VERSION,

            am: {
                transmitters:
                    rooms.am.transmitters.size,

                receivers:
                    rooms.am.receivers.size,

                activeCasterId:
                    getActiveCasterId(rooms.am),

                sampleRate:
                    rooms.am.config.sampleRate,

                channels:
                    rooms.am.config.channels
            },

            fm: {
                transmitters:
                    rooms.fm.transmitters.size,

                receivers:
                    rooms.fm.receivers.size,

                activeCasterId:
                    getActiveCasterId(rooms.fm),

                sampleRate:
                    rooms.fm.config.sampleRate,

                channels:
                    rooms.fm.config.channels
            },

            timestamp:
                new Date().toISOString()

        }));

        return;
    }


    // --------------------------------------------------------
    // ROOT
    // --------------------------------------------------------

    res.writeHead(200, {
        "Content-Type": "text/plain; charset=utf-8",
        "Cache-Control": "no-cache"
    });

    res.end(
        "AudioBridge Multi-Stream Server Running\n\n" +

        "Server:\n" +
        `${SERVER_NAME} ${SERVER_VERSION}\n\n` +

        "WebSocket Endpoints:\n\n" +

        "AM Transmitter:\n" +
        "/amtx\n\n" +

        "AM Receiver:\n" +
        "/amrx\n\n" +

        "FM Transmitter:\n" +
        "/fmtx\n\n" +

        "FM Receiver:\n" +
        "/fmrx\n\n" +

        "HTTP:\n" +
        "/health\n" +
        "/status\n"
    );
});


// ============================================================
// WEBSOCKET SERVER
// ============================================================

const wss = new WebSocket.Server({

    noServer: true,

    // Audio should NOT be compressed.
    // Compression adds CPU overhead and latency.
    perMessageDeflate: false,

    // Prevent excessive incoming WebSocket frame sizes.
    maxPayload: 2 * 1024 * 1024
});


// ============================================================
// WEBSOCKET UPGRADE
// ============================================================

server.on("upgrade", (request, socket, head) => {

    let url;

    try {

        url = new URL(
            request.url,
            `http://${request.headers.host || "localhost"}`
        );

    } catch (error) {

        socket.write(
            "HTTP/1.1 400 Bad Request\r\n" +
            "Connection: close\r\n" +
            "\r\n"
        );

        socket.destroy();

        return;
    }


    const pathname =
        url.pathname.toLowerCase();


    // --------------------------------------------------------
    // VALID ENDPOINTS
    // --------------------------------------------------------

    const validPaths = new Set([
        "/amtx",
        "/amrx",
        "/fmtx",
        "/fmrx"
    ]);


    if (!validPaths.has(pathname)) {

        socket.write(
            "HTTP/1.1 404 Not Found\r\n" +
            "Connection: close\r\n" +
            "\r\n"
        );

        socket.destroy();

        return;
    }


    // --------------------------------------------------------
    // HANDLE WEBSOCKET
    // --------------------------------------------------------

    wss.handleUpgrade(
        request,
        socket,
        head,
        (ws) => {

            wss.emit(
                "connection",
                ws,
                request
            );

        }
    );
});


// ============================================================
// CONNECTION
// ============================================================

wss.on("connection", (ws, req) => {

    const clientIp =
        req.headers["x-forwarded-for"] ||
        req.socket.remoteAddress;


    let url;

    try {

        url = new URL(
            req.url,
            `http://${req.headers.host || "localhost"}`
        );

    } catch (error) {

        closeSocket(
            ws,
            1008,
            "Invalid URL"
        );

        return;
    }


    const pathname =
        url.pathname.toLowerCase();


    // ========================================================
    // DETERMINE STATION
    // ========================================================

    const station =
        getRoomFromPath(pathname);


    const isTransmitter =
        isTransmitterPath(pathname);


    const isReceiver =
        isReceiverPath(pathname);


    // ========================================================
    // VALIDATION
    // ========================================================

    if (
        !station ||
        (!isTransmitter && !isReceiver)
    ) {

        closeSocket(
            ws,
            1008,
            "Invalid WebSocket endpoint"
        );

        return;
    }


    const room =
        rooms[station];


    // ========================================================
    // CLIENT STATE
    // ========================================================

    ws.stationRoom = station;

    ws.endpoint = pathname;

    ws.isTransmitter =
        isTransmitter;

    ws.isReceiver =
        isReceiver;

    ws.registered =
        !isTransmitter;

    ws.casterId = null;

    ws.stationName = null;

    ws.format = null;

    ws.bitrate = null;

    ws.sampleRate =
        room.config.sampleRate;

    ws.channels =
        room.config.channels;

    ws.isAlive = true;

    ws.registrationTimer = null;


    console.log(
        `[+] ${isTransmitter ? "TRANSMITTER" : "RECEIVER"} ` +
        `${station.toUpperCase()} ` +
        `connected from ${clientIp}`
    );


    // ========================================================
    // HEARTBEAT
    // ========================================================

    ws.on("pong", () => {
        ws.isAlive = true;
    });


    // ========================================================
    // RECEIVER
    // ========================================================

    if (isReceiver) {

        room.receivers.add(ws);

        console.log(
            `[RX ${station.toUpperCase()}] ` +
            `Receiver connected. ` +
            `Active receivers: ${room.receivers.size}`
        );


        sendJson(ws, {
            type: "status",
            role: "receiver",
            station: station.toUpperCase(),
            sampleRate: room.config.sampleRate,
            channels: room.config.channels,
            endpoint: pathname,
            server: SERVER_NAME,
            version: SERVER_VERSION
        });
    }


    // ========================================================
    // TRANSMITTER
    // ========================================================

    if (isTransmitter) {

        /*
         * IMPORTANT:
         *
         * Do NOT add the transmitter to room.transmitters yet.
         *
         * The caster must first send:
         *
         * register-transmitter
         *
         * and receive:
         *
         * transmitter-accepted
         */

        ws.registrationTimer =
            setTimeout(() => {

                if (
                    ws.readyState === WebSocket.OPEN &&
                    !ws.registered
                ) {

                    console.log(
                        `[TX ${station.toUpperCase()}] ` +
                        `Registration timeout from ${clientIp}`
                    );

                    sendJson(ws, {
                        type: "transmitter-rejected",
                        reason: "REGISTRATION_TIMEOUT"
                    });

                    closeSocket(
                        ws,
                        1008,
                        "Transmitter registration timeout"
                    );
                }

            }, REGISTRATION_TIMEOUT_MS);
    }


    // ========================================================
    // RECEIVE DATA
    // ========================================================

    ws.on("message", (message, isBinary) => {

        // ====================================================
        // RECEIVER CANNOT SEND DATA
        // ====================================================

        if (ws.isReceiver) {

            console.warn(
                `[RX ${station.toUpperCase()}] ` +
                `Ignored data from receiver`
            );

            return;
        }


        // ====================================================
        // TRANSMITTER REGISTRATION
        // ====================================================

        if (
            ws.isTransmitter &&
            !ws.registered
        ) {

            if (isBinary) {

                console.warn(
                    `[TX ${station.toUpperCase()}] ` +
                    `Binary data received before registration`
                );

                sendJson(ws, {
                    type: "transmitter-rejected",
                    reason: "REGISTRATION_REQUIRED"
                });

                closeSocket(
                    ws,
                    1008,
                    "Register transmitter before sending audio"
                );

                return;
            }


            let payload;

            try {

                payload =
                    JSON.parse(
                        message.toString()
                    );

            } catch (error) {

                sendJson(ws, {
                    type: "transmitter-rejected",
                    reason: "INVALID_REGISTRATION_JSON"
                });

                closeSocket(
                    ws,
                    1008,
                    "Invalid registration JSON"
                );

                return;
            }


            if (
                !payload ||
                payload.type !==
                    "register-transmitter"
            ) {

                sendJson(ws, {
                    type: "transmitter-rejected",
                    reason: "REGISTRATION_REQUIRED"
                });

                closeSocket(
                    ws,
                    1008,
                    "register-transmitter required"
                );

                return;
            }


            // =================================================
            // VALIDATE CASTER ID
            // =================================================

            const casterId =
                String(
                    payload.casterId || ""
                ).trim();


            if (!casterId) {

                sendJson(ws, {
                    type: "transmitter-rejected",
                    reason: "MISSING_CASTER_ID"
                });

                closeSocket(
                    ws,
                    1008,
                    "casterId is required"
                );

                return;
            }


            // =================================================
            // VALIDATE CHANNEL
            // =================================================

            const requestedChannel =
                String(
                    payload.channel || ""
                )
                .trim()
                .toLowerCase();


            if (
                requestedChannel &&
                requestedChannel !== station
            ) {

                sendJson(ws, {
                    type: "transmitter-rejected",
                    reason: "CHANNEL_MISMATCH",
                    expectedChannel:
                        station.toUpperCase()
                });

                closeSocket(
                    ws,
                    1008,
                    "Channel does not match endpoint"
                );

                return;
            }


            // =================================================
            // ONLY ONE ACTIVE TRANSMITTER PER ROOM
            // =================================================

            const existingTransmitter =
                Array.from(
                    room.transmitters
                ).find(
                    tx =>
                        tx.registered &&
                        tx.readyState ===
                            WebSocket.OPEN
                );


            if (existingTransmitter) {

                const activeCasterId =
                    existingTransmitter.casterId ||
                    "UNKNOWN";


                console.log(
                    `[TX ${station.toUpperCase()}] ` +
                    `Rejected ${casterId}; ` +
                    `active transmitter: ${activeCasterId}`
                );


                sendJson(ws, {

                    type:
                        "transmitter-rejected",

                    reason:
                        "ANOTHER_TX_ACTIVE",

                    activeCasterId:
                        activeCasterId
                });


                closeSocket(
                    ws,
                    1008,
                    "Another transmitter is already active"
                );

                return;
            }


            // =================================================
            // ACCEPT REGISTRATION
            // =================================================

            ws.casterId =
                casterId;

            ws.stationName =
                String(
                    payload.station || ""
                ).trim();

            ws.format =
                String(
                    payload.format || "Opus"
                ).trim();

            ws.bitrate =
                String(
                    payload.bitrate || "128 kbps"
                ).trim();

            ws.sampleRate =
                Number(
                    payload.sampleRate ||
                    room.config.sampleRate
                );

            ws.channels =
                Number(
                    payload.channels ||
                    room.config.channels
                );

            ws.registered = true;


            if (ws.registrationTimer) {

                clearTimeout(
                    ws.registrationTimer
                );

                ws.registrationTimer = null;
            }


            room.transmitters.add(ws);


            console.log(
                `[TX ${station.toUpperCase()}] ` +
                `ACCEPTED caster=${ws.casterId} ` +
                `station="${ws.stationName}" ` +
                `format=${ws.format} ` +
                `bitrate=${ws.bitrate} ` +
                `sampleRate=${ws.sampleRate} ` +
                `channels=${ws.channels}`
            );


            // =================================================
            // ACCEPT RESPONSE
            // =================================================

            sendJson(ws, {

                type:
                    "transmitter-accepted",

                role:
                    "transmitter",

                station:
                    station.toUpperCase(),

                casterId:
                    ws.casterId,

                sampleRate:
                    room.config.sampleRate,

                channels:
                    room.config.channels,

                endpoint:
                    pathname,

                server:
                    SERVER_NAME,

                version:
                    SERVER_VERSION
            });


            // =================================================
            // DO NOT PROCESS REGISTRATION AS AUDIO
            // =================================================

            return;
        }


        // ====================================================
        // TRANSMITTER MUST BE REGISTERED
        // ====================================================

        if (
            ws.isTransmitter &&
            !ws.registered
        ) {

            return;
        }


        // ====================================================
        // ONLY BINARY AUDIO IS ACCEPTED AFTER REGISTRATION
        // ====================================================

        if (!isBinary) {

            console.warn(
                `[TX ${station.toUpperCase()}] ` +
                `Ignoring non-binary message ` +
                `from ${ws.casterId || "unknown"}`
            );

            return;
        }


        // ====================================================
        // CURRENT ROOM
        // ====================================================

        const currentRoom =
            rooms[ws.stationRoom];


        if (!currentRoom) {
            return;
        }


        // ====================================================
        // BROADCAST AUDIO TO RECEIVERS
        // ====================================================

        let delivered = 0;
        let skipped = 0;


        currentRoom.receivers.forEach(
            (receiver) => {

                if (
                    receiver.readyState !==
                    WebSocket.OPEN
                ) {

                    skipped++;
                    return;
                }


                // ------------------------------------------------
                // Protect server from a stalled receiver.
                // ------------------------------------------------

                if (
                    receiver.bufferedAmount >
                    MAX_RECEIVER_BUFFERED_BYTES
                ) {

                    skipped++;

                    console.warn(
                        `[RX ${station.toUpperCase()}] ` +
                        `Slow receiver detected; ` +
                        `buffered=${receiver.bufferedAmount} bytes`
                    );

                    try {

                        receiver.close(
                            1008,
                            "Receiver too slow"
                        );

                    } catch (_) {
                        try {
                            receiver.terminate();
                        } catch (_) {
                            // Ignore.
                        }
                    }

                    return;
                }


                try {

                    receiver.send(
                        message,
                        {
                            binary: true
                        }
                    );

                    delivered++;

                } catch (error) {

                    skipped++;

                    console.error(
                        `[SEND ERROR ${station.toUpperCase()}] ` +
                        `${error.message}`
                    );

                    try {
                        receiver.terminate();
                    } catch (_) {
                        // Ignore.
                    }
                }

            }
        );


        // --------------------------------------------------------
        // Optional low-frequency diagnostic.
        // Do NOT log every audio packet.
        // --------------------------------------------------------

        ws.audioPackets =
            (ws.audioPackets || 0) + 1;

        ws.audioBytes =
            (ws.audioBytes || 0) +
            message.length;

        if (
            ws.audioPackets % 500 === 0
        ) {

            console.log(
                `[AUDIO ${station.toUpperCase()}] ` +
                `caster=${ws.casterId} ` +
                `packets=${ws.audioPackets} ` +
                `bytes=${ws.audioBytes} ` +
                `receivers=${currentRoom.receivers.size} ` +
                `delivered=${delivered} ` +
                `skipped=${skipped}`
            );
        }

    });


    // ========================================================
    // CLOSE
    // ========================================================

    ws.on("close", (code, reasonBuffer) => {

        if (ws.registrationTimer) {

            clearTimeout(
                ws.registrationTimer
            );

            ws.registrationTimer = null;
        }


        const reason =
            reasonBuffer
                ? reasonBuffer.toString()
                : "";


        removeFromRoom(ws);


        if (ws.isTransmitter) {

            console.log(
                `[-] TX ` +
                `${ws.stationRoom.toUpperCase()} ` +
                `caster=${ws.casterId || "unregistered"} ` +
                `disconnected ` +
                `(code ${code}` +
                `${reason ? `, reason=${reason}` : ""})`
            );

        } else {

            console.log(
                `[-] RX ` +
                `${ws.stationRoom.toUpperCase()} ` +
                `disconnected ` +
                `(code ${code}` +
                `${reason ? `, reason=${reason}` : ""})`
            );
        }

    });


    // ========================================================
    // ERROR
    // ========================================================

    ws.on("error", (error) => {

        console.error(
            `[WS ERROR ${station.toUpperCase()} ` +
            `${ws.casterId || clientIp}]`,
            error.message
        );

    });

});


// ============================================================
// HEARTBEAT TIMER
// ============================================================

const heartbeatTimer =
    setInterval(() => {

        wss.clients.forEach((ws) => {

            if (
                ws.readyState !==
                WebSocket.OPEN
            ) {
                return;
            }


            // ------------------------------------------------
            // Dead connection
            // ------------------------------------------------

            if (ws.isAlive === false) {

                console.log(
                    `[TIMEOUT] ` +
                    `${ws.stationRoom || "UNKNOWN"} ` +
                    `${ws.casterId || "client"}`
                );

                try {
                    ws.terminate();
                } catch (_) {
                    // Ignore.
                }

                return;
            }


            ws.isAlive = false;


            // ------------------------------------------------
            // Ping
            // ------------------------------------------------

            try {
                ws.ping();
            } catch (error) {

                console.error(
                    `[PING ERROR]`,
                    error.message
                );

                try {
                    ws.terminate();
                } catch (_) {
                    // Ignore.
                }
            }

        });

    }, HEARTBEAT_INTERVAL_MS);


// ============================================================
// WEBSOCKET SERVER CLOSE
// ============================================================

wss.on("close", () => {

    clearInterval(
        heartbeatTimer
    );

});


// ============================================================
// GRACEFUL SHUTDOWN
// ============================================================

function gracefulShutdown(signal) {

    console.log(
        `${signal} received. Shutting down...`
    );


    clearInterval(
        heartbeatTimer
    );


    wss.clients.forEach((ws) => {

        try {

            ws.close(
                1001,
                "Server shutting down"
            );

        } catch (_) {

            try {
                ws.terminate();
            } catch (_) {
                // Ignore.
            }

        }

    });


    server.close(() => {

        console.log(
            "Server closed."
        );

        process.exit(0);

    });


    // Safety fallback.
    setTimeout(() => {

        process.exit(0);

    }, 5000);
}


process.on(
    "SIGTERM",
    () => gracefulShutdown("SIGTERM")
);

process.on(
    "SIGINT",
    () => gracefulShutdown("SIGINT")
);


// ============================================================
// START
// ============================================================

server.listen(
    PORT,
    "0.0.0.0",
    () => {

        console.log("");
        console.log(
            "======================================"
        );

        console.log(
            "       AUDIOBRIDGE SERVER"
        );

        console.log(
            "======================================"
        );

        console.log(
            `VERSION: ${SERVER_VERSION}`
        );

        console.log(
            `PORT: ${PORT}`
        );

        console.log("");

        console.log(
            "AM TX: /amtx"
        );

        console.log(
            "AM RX: /amrx"
        );

        console.log("");

        console.log(
            "FM TX: /fmtx"
        );

        console.log(
            "FM RX: /fmrx"
        );

        console.log("");

        console.log(
            "HEALTH: /health"
        );

        console.log(
            "STATUS: /status"
        );

        console.log("");

        console.log(
            "Registration protocol: ENABLED"
        );

        console.log(
            "One active transmitter per station: ENABLED"
        );

        console.log(
            "WebSocket heartbeat: ENABLED"
        );

        console.log(
            "Audio compression: DISABLED"
        );

        console.log("");

        console.log(
            "======================================"
        );

        console.log("");

    }
);