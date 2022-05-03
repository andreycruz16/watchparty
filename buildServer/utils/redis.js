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
exports.getRedisCountHourDistinct = exports.getRedisCountDayDistinct = exports.redisCountDistinct = exports.getRedisCountHour = exports.getRedisCountDay = exports.redisCount = void 0;
const config_1 = __importDefault(require("../config"));
const ioredis_1 = __importDefault(require("ioredis"));
const time_1 = require("./time");
let redis = undefined;
if (config_1.default.REDIS_URL) {
    redis = new ioredis_1.default(config_1.default.REDIS_URL);
}
function redisCount(prefix) {
    return __awaiter(this, void 0, void 0, function* () {
        if (!redis) {
            return;
        }
        const key = `${prefix}:${(0, time_1.getStartOfHour)()}`;
        yield redis.incr(key);
        yield redis.expireat(key, (0, time_1.getStartOfHour)() + 86400 * 1000);
    });
}
exports.redisCount = redisCount;
function getRedisCountDay(prefix) {
    return __awaiter(this, void 0, void 0, function* () {
        if (!redis) {
            return;
        }
        // Get counts for last 24 hour keys (including current partial hour)
        const keyArr = [];
        for (let i = 0; i < 24; i += 1) {
            keyArr.push(`${prefix}:${(0, time_1.getStartOfHour)() - i * 3600 * 1000}`);
        }
        const values = yield redis.mget(...keyArr);
        return values.reduce((a, b) => (Number(a) || 0) + (Number(b) || 0), 0);
    });
}
exports.getRedisCountDay = getRedisCountDay;
function getRedisCountHour(prefix) {
    return __awaiter(this, void 0, void 0, function* () {
        if (!redis) {
            return;
        }
        // Get counts for previous full hour
        const value = yield redis.get(`${prefix}:${(0, time_1.getStartOfHour)() - 3600 * 1000}`);
        return Number(value);
    });
}
exports.getRedisCountHour = getRedisCountHour;
function redisCountDistinct(prefix, item) {
    return __awaiter(this, void 0, void 0, function* () {
        if (!redis) {
            return;
        }
        const key = `${prefix}:${(0, time_1.getStartOfHour)()}`;
        yield redis.pfadd(key, item);
        yield redis.expireat(key, (0, time_1.getStartOfHour)() + 86400 * 1000);
    });
}
exports.redisCountDistinct = redisCountDistinct;
function getRedisCountDayDistinct(prefix) {
    return __awaiter(this, void 0, void 0, function* () {
        if (!redis) {
            return;
        }
        // Get counts for last 24 hour keys (including current partial hour)
        const keyArr = [];
        for (let i = 0; i < 24; i += 1) {
            keyArr.push(`${prefix}:${(0, time_1.getStartOfHour)() - i * 3600 * 1000}`);
        }
        return yield redis.pfcount(...keyArr);
    });
}
exports.getRedisCountDayDistinct = getRedisCountDayDistinct;
function getRedisCountHourDistinct(prefix) {
    return __awaiter(this, void 0, void 0, function* () {
        if (!redis) {
            return;
        }
        // Get counts for previous full hour
        return yield redis.pfcount(`${prefix}:${(0, time_1.getStartOfHour)() - 3600 * 1000}`);
    });
}
exports.getRedisCountHourDistinct = getRedisCountHourDistinct;
