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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const pg_1 = require("pg");
const config_1 = __importDefault(require("./config"));
const postgres = new pg_1.Client({
    connectionString: config_1.default.DATABASE_URL,
    ssl: { rejectUnauthorized: false },
});
postgres.connect();
setInterval(cleanupPostgres, 5 * 60 * 1000);
function cleanupPostgres() {
    return __awaiter(this, void 0, void 0, function* () {
        if (!postgres) {
            return;
        }
        console.time('[CLEANUP]');
        yield (postgres === null || postgres === void 0 ? void 0 : postgres.query(`DELETE FROM room WHERE owner IS NULL AND "lastUpdateTime" < NOW() - INTERVAL '1 day'`));
        console.timeEnd('[CLEANUP]');
    });
}
