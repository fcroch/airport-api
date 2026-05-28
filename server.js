const express = require('express');
const mysql = require('mysql2/promise');
const crypto = require('crypto');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

// CONFIGURAÇÃO DO BANCO DE DADOS
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

// Testador e Migração Automática
pool.getConnection()
    .then(conn => {
        console.log("✅ BANCO DE DADOS CONECTADO COM SUCESSO!");
        // Cria a coluna de WhatsApp automaticamente se ela não existir
        conn.execute('ALTER TABLE Users ADD COLUMN phone VARCHAR(20) DEFAULT NULL').catch(() => {});
        conn.release();
    })
    .catch(err => {
        console.error("❌ ERRO NO BANCO:", err);
    });

// ==========================================
// MÓDULO WEBHOOK WHATSAPP API
// ==========================================
async function sendWhatsAppAlert(phone, message) {
    if (!phone) return;
    
    // Substitua pelo URL da sua API (Evolution API, Z-API, etc) e configure seus cabeçalhos/token
    const API_URL = "URL_DA_SUA_API_AQUI"; 
    
    try {
        console.log(`[ALERTA WHATSAPP] Enviando para ${phone}: ${message}`);
        /* DESCOMENTE QUANDO TIVER A SUA API
        await fetch(API_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ number: phone, text: message })
        });
        */
    } catch (e) {
        console.error("Erro ao disparar WhatsApp:", e.message);
    }
}

// ==========================================
// MÓDULO DE TEMPO REAL (SSE)
// ==========================================
const sseClients = new Map();

function broadcastToSession(sessionId, eventType, data) {
    const id = String(sessionId); 
    if (sseClients.has(id)) {
        const clients = sseClients.get(id);
        const payload = `event: ${eventType}\ndata: ${JSON.stringify(data)}\n\n`;
        clients.forEach(client => client.write(payload));
    }
}

app.get('/api/stream/:sessionId', (req, res) => {
    const sessionId = String(req.params.sessionId);

    res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Access-Control-Allow-Origin': '*'
    });

    if (!sseClients.has(sessionId)) {
        sseClients.set(sessionId, new Set());
    }
    
    sseClients.get(sessionId).add(res);

    const keepAlive = setInterval(() => {
        res.write(': keepalive\n\n');
    }, 15000);

    req.on('close', () => {
        clearInterval(keepAlive);
        if (sseClients.has(sessionId)) {
            sseClients.get(sessionId).delete(res);
            if (sseClients.get(sessionId).size === 0) {
                sseClients.delete(sessionId);
            }
        }
    });
});

// ==========================================
// ROTAS DA API
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
            [tracking_code, passenger_id, flight_status, terminal, arrival_gate]
        );

        res.status(201).json({
            message: 'Sessão criada com sucesso',
            session_id: result.insertId,
            tracking_code,
            passenger_id
        });
    } catch (error) {
        res.status(500).json({ error: 'Erro ao criar sessão' });
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

        res.json({ message: 'Conectado à sessão', seeker_id, passenger_id: session.passenger_id });
    } catch (error) {
        res.status(500).json({ error: 'Erro ao entrar na sessão' });
    }
});

// Atualizar Status (Passageiro clica nos botões)
app.patch('/api/meetups/:code/status', async (req, res) => {
    const { code } = req.params;
    const { flight_status } = req.body;

    try {
        const [rows] = await pool.execute('SELECT id FROM MeetupSessions WHERE tracking_code = ?', [code]);
        if (rows.length === 0) return res.status(404).json({ error: 'Sessão não encontrada.' });
        
        const session_id = rows[0].id;

        await pool.execute(
            'UPDATE MeetupSessions SET flight_status = ? WHERE id = ?',
            [flight_status, session_id]
        );

        // Dispara evento em tempo real
        broadcastToSession(session_id, 'status_update', { flight_status });

        // GATILHO DE WHATSAPP:
        if (flight_status === 'No Saguão' || flight_status === 'Desembarcando') {
            const [seekerRows] = await pool.execute('SELECT u.phone FROM Users u JOIN MeetupSessions m ON m.seeker_id = u.id WHERE m.id = ?', [session_id]);
            if (seekerRows.length > 0 && seekerRows[0].phone) {
                sendWhatsAppAlert(seekerRows[0].phone, `⚠️ Alerta AeroMeet: O Passageiro do código [${code}] mudou o status para: ${flight_status}`);
            }
        }

        res.json({ message: 'Status atualizado' });
    } catch (error) {
        res.status(500).json({ error: 'Erro ao atualizar status' });
    }
});

// Receber localização do GPS
app.post('/api/pings', async (req, res) => {
    const { session_id, user_id, latitude, longitude } = req.body;

    try {
        await pool.execute(
            'INSERT INTO LocationPings (session_id, user_id, latitude, longitude) VALUES (?, ?, ?, ?)',
            [session_id, user_id, latitude, longitude]
        );

        broadcastToSession(session_id, 'location_update', { user_id, latitude, longitude });

        res.json({ success: true });
    } catch (error) {
        res.status(500).json({ error: 'Erro ao salvar ping' });
    }
});

app.get('/api/meetups/:code', async (req, res) => {
    try {
        const [rows] = await pool.execute('SELECT * FROM MeetupSessions WHERE tracking_code = ?', [req.params.code]);
        if (rows.length === 0) return res.status(404).json({ error: 'Não encontrado' });
        res.json(rows[0]);
    } catch (error) {
        res.status(500).json({ error: 'Erro interno' });
    }
});

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log(`Servidor rodando na porta ${PORT}`));
