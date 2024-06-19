import express from 'express';
import { loggedInAsAdmin } from '../auth/authentication.js';
import {
  purgeHorses,
  getServerStatus,
  openRaceServer,
  closeRaceServer,
} from '../controllers/admin.js';

const router = express.Router();

router.get('/serverStatus', getServerStatus);

// router.post('/openServer', loggedInAsAdmin, openRaceServer);
/* runs the server just one time **/
router.post('/openServer', openRaceServer);

router.post('/closeServer', loggedInAsAdmin, closeRaceServer);

router.post('/newHorses', loggedInAsAdmin, purgeHorses);

export default router;
