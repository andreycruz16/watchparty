"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    Object.defineProperty(o, k2, { enumerable: true, get: function() { return m[k]; } });
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.deleteUser = exports.getUserEmail = exports.getUser = exports.getUserByEmail = exports.writeData = exports.validateUserToken = void 0;
const config_1 = __importDefault(require("../config"));
const admin = __importStar(require("firebase-admin"));
if (config_1.default.FIREBASE_ADMIN_SDK_CONFIG) {
    admin.initializeApp({
        credential: admin.credential.cert(JSON.parse(config_1.default.FIREBASE_ADMIN_SDK_CONFIG)),
        databaseURL: config_1.default.FIREBASE_DATABASE_URL,
    });
}
function validateUserToken(uid, token) {
    return __awaiter(this, void 0, void 0, function* () {
        if (!config_1.default.FIREBASE_ADMIN_SDK_CONFIG) {
            return undefined;
        }
        if (!token) {
            return undefined;
        }
        const decoded = yield admin.auth().verifyIdToken(token);
        if (uid !== decoded.uid) {
            return undefined;
        }
        return decoded;
    });
}
exports.validateUserToken = validateUserToken;
function writeData(key, value) {
    return __awaiter(this, void 0, void 0, function* () {
        if (!config_1.default.FIREBASE_ADMIN_SDK_CONFIG) {
            return;
        }
        yield admin.database().ref(key).set(value);
    });
}
exports.writeData = writeData;
function getUserByEmail(email) {
    return __awaiter(this, void 0, void 0, function* () {
        if (!config_1.default.FIREBASE_ADMIN_SDK_CONFIG) {
            return null;
        }
        try {
            return yield admin.auth().getUserByEmail(email);
        }
        catch (e) {
            console.log(email, e.message);
        }
        return null;
    });
}
exports.getUserByEmail = getUserByEmail;
function getUser(uid) {
    return __awaiter(this, void 0, void 0, function* () {
        if (!config_1.default.FIREBASE_ADMIN_SDK_CONFIG) {
            return null;
        }
        return yield admin.auth().getUser(uid);
    });
}
exports.getUser = getUser;
function getUserEmail(uid) {
    return __awaiter(this, void 0, void 0, function* () {
        if (!config_1.default.FIREBASE_ADMIN_SDK_CONFIG) {
            return null;
        }
        const user = yield admin.auth().getUser(uid);
        return user.email;
    });
}
exports.getUserEmail = getUserEmail;
function deleteUser(uid) {
    return __awaiter(this, void 0, void 0, function* () {
        if (!config_1.default.FIREBASE_ADMIN_SDK_CONFIG) {
            return null;
        }
        return admin.auth().deleteUser(uid);
    });
}
exports.deleteUser = deleteUser;
