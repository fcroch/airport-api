const express = require('express');
const mysql = require('mysql2/promise');
const crypto = require('crypto');
const cors = require('cors');
const app = express();
app.use(cors());
app.use(express.json());
const dbConfig = {
    host: '82.112.247.235',
    user: 'u809350891_admin_user',
    password: 'KNTjHWDwRckXxq6',
    database: 'u809350891_topclassexec'
};
const pool = mysql.createPool({
    ...dbConfig,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});
// Testador de Conexão Automático
pool.getConnection()
    .then(conn => {
        console.log("✅ BANCO DE DADOS CONECTADO COM SUCESSO!");
        conn.release();
    })
    .catch(err => {
        console.error("❌ O SEGREDO DO ERRO É:");
        console.error(err);
    });
const sseClients = new Map();
function broadcastToSession(sessionId, eventType, data) {
    if (sseClients.has(sessionId)) {
        const clients = sseClients.get(sessionId);
        const payload = `event: ${eventType}\ndata: ${JSON.stringify(data)}\n\n`;
        clients.forEach(client => client.write(payload));
    }
}
app.post('/api/meetups', async (req, res) => {
    let { passenger_id, flight_status, terminal, arrival_gate } = req.body;
    const tracking_code = crypto.randomBytes(3).toString('hex').toUpperCase();
    try {
        if (!passenger_id) {
            const [userResult] = await pool.execute(
                "INSERT INTO Users (name, role) VALUES ('Passageiro Anônimo', 'passenger')"
            );
            passenger_id = userResult.insertId;
        }
        const [result] = await pool.execute(
            `INSERT INTO MeetupSessions 
            (tracking_code, passenger_id, flight_status, terminal, arrival_gate, status) 
            VALUES (?, ?, ?, ?, ?, 'active')`,
            [tracking_code, passenger_id, flight_status || 'Aterrissou', terminal || null, arrival_gate || null]
        );
        res.status(201).json({
            message: 'Sessão criada com sucesso',
            session_id: result.insertId,
            tracking_code,
            passenger_id
        });
    } catch (error) {
        console.error("Erro no POST /api/meetups:", error);
        res.status(500).json({ error: 'Erro ao criar sessão' });
    }
});
app.get('/api/meetups/:trackingCode', async (req, res) => {
    const { trackingCode } = req.params;
    try {
        const [rows] = await pool.execute(
            'SELECT * FROM MeetupSessions WHERE tracking_code = ? LIMIT 1',
            [trackingCode]
        );
        if (rows.length === 0) return res.status(404).json({ error: 'Sessão não encontrada' });
        res.json(rows[0]);
    } catch (error) {
        res.status(500).json({ error: 'Erro ao buscar sessão' });
    }
});
app.patch('/api/meetups/:trackingCode/join', async (req, res) => {
    const { trackingCode } = req.params;
    try {
        const [userResult] = await pool.execute(
            "INSERT INTO Users (name, role) VALUES ('Buscador Anônimo', 'seeker')"
        );
        const seeker_id = userResult.insertId;
        const [result] = await pool.execute(
            'UPDATE MeetupSessions SET seeker_id = ? WHERE tracking_code = ? AND status = "active"',
            [seeker_id, trackingCode]
        );
        
        if (result.affectedRows === 0) {
            return res.status(400).json({ error: 'Sessão inválida ou já finalizada' });
        }
        res.json({ message: 'Sessão vinculada', seeker_id });
    } catch (error) {
        res.status(500).json({ error: 'Erro ao ingressar' });
    }
});
app.patch('/api/meetups/:trackingCode/status', async (req, res) => {
    const { trackingCode } = req.params;
    const { flight_status } = req.body;
    try {
        await pool.execute(
            'UPDATE MeetupSessions SET flight_status = ? WHERE tracking_code = ?',
            [flight_status, trackingCode]
        );
        
        const [rows] = await pool.execute('SELECT id FROM MeetupSessions WHERE tracking_code = ?', [trackingCode]);
        if (rows.length > 0) {
            broadcastToSession(rows[0].id, 'status_update', { flight_status });
        }
        res.json({ message: 'Status atualizado' });
    } catch(e) {
        res.status(500).json({ error: 'Erro interno' });
    }
});
app.post('/api/pings', async (req, res) => {
    const { session_id, user_id, latitude, longitude } = req.body;
    try {
        await pool.execute(
            'INSERT INTO LocationPings (session_id, user_id, latitude, longitude) VALUES (?, ?, ?, ?)',
            [session_id, user_id, latitude, longitude]
        );
        broadcastToSession(session_id, 'location_update', { user_id, latitude, longitude });
        res.status(201).json({ success: true });
    } catch (e) {
        res.status(500).json({ error: 'Erro ao salvar ping' });
    }
});
app.get('/api/stream/:sessionId', (req, res) => {
    const { sessionId } = req.params;
    
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();
    if (!sseClients.has(sessionId)) {
        sseClients.set(sessionId, []);
    }
    sseClients.get(sessionId).push(res);
    req.on('close', () => {
        const clients = sseClients.get(sessionId);
        if (clients) {
            sseClients.set(sessionId, clients.filter(c => c !== res));
        }
    });
});
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
    console.log(`Servidor rodando na porta ${PORT}`);
});
