const express = require('express');
const mysql = require('mysql2/promise');
const crypto = require('crypto');
const cors = require('cors'); // Necessário para a arquitetura separada

const app = express();
app.use(cors()); // Permite que o seu domínio topclassexecutive.com.br converse com a Render
app.use(express.json());

// A pasta public não será mais servida pelo Node, pois o frontend estará direto na Hostinger.

// Configuração do Banco de Dados
const dbConfig = {
    host: '82.112.247.235', // IP do seu servidor MySQL na Hostinger
    user: 'u809350891_admin_user',
    password: 'KNTjHWDwRckXxq6', // <-- Lembre-se de alterar para a senha real antes de subir!
    database: 'u809350891_topclassexec'
};

const pool = mysql.createPool({
    ...dbConfig,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

// Testador de Conexão Automático + Auto-Migração
pool.getConnection()
    .then(conn => {
        console.log("✅ BANCO DE DADOS CONECTADO COM SUCESSO!");
        // Adiciona a coluna phone se não existir, de forma segura
        conn.execute('ALTER TABLE Users ADD COLUMN phone VARCHAR(20) DEFAULT NULL').catch(() => {});
        conn.release();
    })
    .catch(err => {
        console.error("❌ O SEGREDO DO ERRO É:", err);
    });

// ==========================================
// GERENCIAMENTO DE CLIENTES SSE
// ==========================================
const sseClients = new Map();

function broadcastToSession(sessionId, eventType, data) {
    if (sseClients.has(sessionId)) {
        const clients = sseClients.get(sessionId);
        const payload = `event: ${eventType}\ndata: ${JSON.stringify(data)}\n\n`;
        clients.forEach(client => client.write(payload));
    }
}

// ==========================================
// ROTAS PARA MEETUP SESSIONS
// ==========================================

// Criar nova sessão (Passageiro chega)
app.post('/api/meetups', async (req, res) => {
    let { passenger_id, flight_status, terminal, arrival_gate, phone } = req.body;
    const tracking_code = crypto.randomBytes(3).toString('hex').toUpperCase();

    try {
        if (!passenger_id) {
            const [userResult] = await pool.execute(
                "INSERT INTO Users (name, role, phone) VALUES ('Passageiro Anônimo', 'passenger', ?)",
                [phone || null]
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

// Entrar na sessão (Motorista busca)
app.patch('/api/meetups/:code/join', async (req, res) => {
    const { code } = req.params;
    let { seeker_id, phone } = req.body;

    try {
        const [rows] = await pool.execute('SELECT * FROM MeetupSessions WHERE tracking_code = ? AND status = "active"', [code]);
        if (rows.length === 0) return res.status(404).json({ error: 'Sessão não encontrada.' });
        
        const session = rows[0];

        if (!seeker_id) {
            const [userResult] = await pool.execute(
                "INSERT INTO Users (name, role, phone) VALUES ('Motorista Anônimo', 'seeker', ?)",
                [phone || null]
            );
            seeker_id = userResult.insertId;
        }

        await pool.execute(
            'UPDATE MeetupSessions SET seeker_id = ? WHERE id = ?',
            [seeker_id, session.id]
        );

        res.json({ message: 'Conectado com sucesso', seeker_id });
    } catch (error) {
        res.status(500).json({ error: 'Erro ao entrar na sessão' });
    }
});

app.patch('/api/meetups/:trackingCode/status', async (req, res) => {
    const { trackingCode } = req.params;
    const { flight_status, webhook_url } = req.body;

    try {
        const [rows] = await pool.execute('SELECT id FROM MeetupSessions WHERE tracking_code = ?', [trackingCode]);
        if (rows.length === 0) return res.status(404).json({ error: 'Sessão não encontrada' });
        
        const sessionId = rows[0].id;

        await pool.execute('UPDATE MeetupSessions SET flight_status = ? WHERE tracking_code = ?', [flight_status, trackingCode]);
        
        broadcastToSession(sessionId, 'status_update', { flight_status });

        if (webhook_url) {
            fetch(webhook_url, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ event: 'status_update', flight_status, tracking_code: trackingCode })
            }).catch(console.error);
        }

        res.json({ message: 'Status atualizado com sucesso' });
    } catch (error) {
        res.status(500).json({ error: 'Erro ao atualizar status' });
    }
});

// ==========================================
// ROTAS PARA LOCATION PINGS E SSE
// ==========================================

app.get('/api/stream/:sessionId', (req, res) => {
    const sessionId = parseInt(req.params.sessionId);

    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();

    if (!sseClients.has(sessionId)) {
        sseClients.set(sessionId, new Set());
    }
    sseClients.get(sessionId).add(res);

    req.on('close', () => {
        const clients = sseClients.get(sessionId);
        if (clients) {
            clients.delete(res);
            if (clients.size === 0) sseClients.delete(sessionId);
        }
    });
});

app.post('/api/pings', async (req, res) => {
    const { session_id, user_id, latitude, longitude } = req.body;

    try {
        const [result] = await pool.execute(
            'INSERT INTO LocationPings (session_id, user_id, latitude, longitude) VALUES (?, ?, ?, ?)',
            [session_id, user_id, latitude, longitude]
        );

        const pingData = { id: result.insertId, latitude, longitude, timestamp: new Date() };
        broadcastToSession(session_id, 'location_update', pingData);

        res.status(201).json({ success: true, ping_id: result.insertId });
    } catch (error) {
        console.error("Erro no POST /api/pings:", error);
        res.status(500).json({ error: 'Erro ao salvar localização' });
    }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`Servidor rodando na porta ${PORT}`);
});
