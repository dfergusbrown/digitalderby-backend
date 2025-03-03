import 'dotenv/config';
import express from 'express';
import { createServer } from 'node:http';
import gameServer from './game/gameServer.js';

import './config/database.js';

import cors from 'cors';
import corsSettings from './cors.js';
import logger from 'morgan';

import AdminRouter from './routes/admin.js';
import AuthRouter from './routes/auth.js';
import HorseRouter from './routes/horses.js';
import UserRouter from './routes/users.js';
import RaceRouter from './routes/races.js';
import bodyParser from 'body-parser';

import swaggerUi from 'swagger-ui-express';
import YAML from 'yamljs';
import { AUTOSTART } from './config/globalsettings.js';
import CreateAdminUser from './auth/admin.js';

export const args = {
  readOnly: false,
};

if (args['read-only']) {
  console.log('Read-only mode enabled');
  args.readOnly = true;
}

const app = express();
export const server = createServer(app);

const PORT = process.env.PORT || 3000;

app.use(logger('dev'));
app.use(cors(corsSettings));
app.use(bodyParser.json());

app.get('/', (_req, res) => {
  res.json({ message: 'this is the index route, the server is working' });
});

app.use('/admin', AdminRouter);
app.use('/auth', AuthRouter);
app.use('/horses', HorseRouter);
app.use('/users', UserRouter);
app.use('/races', RaceRouter);

const swaggerOptions = {
  swaggerOptions: {
    supportedSubmitMethods: [],
  },
};

app.use(
  '/api-docs',
  swaggerUi.serve,
  swaggerUi.setup(YAML.load('./api-docs.yaml'), swaggerOptions),
);

await CreateAdminUser();

gameServer.createServer(server);

if (AUTOSTART) {
  gameServer.openServer();
  gameServer.startMainLoop();
}

server.listen(PORT, () => {
  console.log(`listening at http://localhost:${PORT}`);
  console.log(`Access api documentation at http://localhost:${PORT}/api-docs`);
});
