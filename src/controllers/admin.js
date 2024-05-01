"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.createNewHorses = exports.toggleAutostart = exports.startRaceAndAutostart = exports.startRaceLoop = exports.closeRaceServer = exports.openRaceServer = exports.getServerStatus = exports.getServerSettings = exports.loginAsAdmin = void 0;
var jsonwebtoken_1 = require("jsonwebtoken");
var gameServer_js_1 = require("../game/gameServer.js");
var password_js_1 = require("../auth/password.js");
var secrets_js_1 = require("../auth/secrets.js");
var Horse_js_1 = require("../models/Horse.js");
var errorHandler_js_1 = require("../errorHandler.js");
var localHorses_js_1 = require("../game/horse/localHorses.js");
function loginAsAdmin(req, res, next) {
    return __awaiter(this, void 0, void 0, function () {
        var payload, token;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, (0, password_js_1.verifyPassword)(req.body.password, secrets_js_1.adminPasswordHash)];
                case 1:
                    if (!(_a.sent())) {
                        return [2 /*return*/, next({ status: 400, message: 'Could not login as admin' })];
                    }
                    payload = { isAdmin: true };
                    token = jsonwebtoken_1.default.sign(payload, secrets_js_1.jwtSecret, { expiresIn: '1d' });
                    res.status(200).json({
                        message: 'Created admin token',
                        token: token,
                    });
                    return [2 /*return*/];
            }
        });
    });
}
exports.loginAsAdmin = loginAsAdmin;
function getServerSettings(req, res, next) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            res.status(200).json({
                message: 'Server settings'
            });
            return [2 /*return*/];
        });
    });
}
exports.getServerSettings = getServerSettings;
function getServerStatus(req, res, next) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            res.status(200).json({ serverStatus: gameServer_js_1.default.serverStatus });
            return [2 /*return*/];
        });
    });
}
exports.getServerStatus = getServerStatus;
function openRaceServer(req, res, next) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            gameServer_js_1.default.openServer();
            res.status(200).json({ message: 'Server is now active' });
            return [2 /*return*/];
        });
    });
}
exports.openRaceServer = openRaceServer;
function closeRaceServer(req, res, next) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            try {
                gameServer_js_1.default.closeServer();
                res.status(200).json({ message: 'Server is now inactive' });
            }
            catch (error) {
                (0, errorHandler_js_1.sendJSONError)(res, 500, "Could not close the server");
            }
            return [2 /*return*/];
        });
    });
}
exports.closeRaceServer = closeRaceServer;
function startRaceLoop(req, res, next) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            gameServer_js_1.default.startMainLoop();
            res.status(200).json({ message: 'Server main loop has begun' });
            return [2 /*return*/];
        });
    });
}
exports.startRaceLoop = startRaceLoop;
function startRaceAndAutostart(req, res, next) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            res.status(200).json({
                message: 'Server settings'
            });
            return [2 /*return*/];
        });
    });
}
exports.startRaceAndAutostart = startRaceAndAutostart;
function toggleAutostart(req, res, next) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            res.status(200).json({
                message: 'Server settings'
            });
            return [2 /*return*/];
        });
    });
}
exports.toggleAutostart = toggleAutostart;
function createNewHorses(req, res, next) {
    return __awaiter(this, void 0, void 0, function () {
        var horses, error_1;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    _a.trys.push([0, 3, , 4]);
                    // Delete all horses already in the database first
                    return [4 /*yield*/, Horse_js_1.Horse.deleteMany({})];
                case 1:
                    // Delete all horses already in the database first
                    _a.sent();
                    horses = (0, Horse_js_1.generateNewHorses)()
                        .map(function (h) { return new Horse_js_1.Horse(h); });
                    (0, localHorses_js_1.generateLocalHorsesFromSpecs)(horses);
                    return [4 /*yield*/, Promise.all(horses.map(function (hm) { return hm.save(); }))];
                case 2:
                    _a.sent();
                    res.status(200).json({
                        message: 'Successfully created a new generation of horses'
                    });
                    return [3 /*break*/, 4];
                case 3:
                    error_1 = _a.sent();
                    (0, errorHandler_js_1.sendJSONError)(res, 500, "Internal error creating horses: ".concat(error_1));
                    return [3 /*break*/, 4];
                case 4: return [2 /*return*/];
            }
        });
    });
}
exports.createNewHorses = createNewHorses;
