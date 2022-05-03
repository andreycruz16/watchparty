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
const config_1 = __importDefault(require("./config"));
const ioredis_1 = __importDefault(require("ioredis"));
const axios_1 = __importDefault(require("axios"));
const ecosystem_config_1 = __importDefault(require("./ecosystem.config"));
let redis = undefined;
if (config_1.default.REDIS_URL) {
    redis = new ioredis_1.default(config_1.default.REDIS_URL);
}
statsTimeSeries();
setInterval(statsTimeSeries, 5 * 60 * 1000);
function statsTimeSeries() {
    return __awaiter(this, void 0, void 0, function* () {
        if (redis) {
            console.time('timeSeries');
            const ports = process.env.NODE_ENV === 'development'
                ? [8080]
                : ecosystem_config_1.default.apps
                    // TODO remove this filter when sharding deployed
                    .filter((app) => app.name === 'server')
                    .map((app) => { var _a; return (_a = app.env) === null || _a === void 0 ? void 0 : _a.PORT; })
                    .filter(Boolean);
            const shardReqs = ports.map((port) => (0, axios_1.default)({
                url: `http://localhost:${port}/stats?key=${config_1.default.STATS_KEY}`,
                validateStatus: () => true,
            }));
            let stats = {};
            try {
                const shardData = yield Promise.all(shardReqs);
                shardData.forEach((shard) => {
                    const data = shard.data;
                    stats = combine(stats, data);
                });
                const datapoint = {
                    time: new Date(),
                    currentUsers: stats.currentUsers,
                    currentVBrowser: stats.currentVBrowser,
                    currentVBrowserLarge: stats.currentVBrowserLarge,
                    currentHttp: stats.currentHttp,
                    currentScreenShare: stats.currentScreenShare,
                    currentFileShare: stats.currentFileShare,
                    currentVideoChat: stats.currentVideoChat,
                    currentRoomCount: stats.currentRoomCount,
                    chatMessages: stats.chatMessages,
                    redisUsage: stats.redisUsage,
                    hetznerApiRemaining: stats.hetznerApiRemaining,
                    avgStartMS: stats.vBrowserStartMS &&
                        stats.vBrowserStartMS.reduce((a, b) => Number(a) + Number(b), 0) / stats.vBrowserStartMS.length,
                    vBrowserStarts: stats.vBrowserStarts,
                    vBrowserLaunches: stats.vBrowserLaunches,
                    vBrowserFails: stats.vBrowserFails,
                    vBrowserStagingFails: stats.vBrowserStagingFails,
                };
                Object.keys(stats.vmManagerStats).forEach((key) => {
                    var _a, _b;
                    if (stats.vmManagerStats[key]) {
                        datapoint[key] =
                            (_b = (_a = stats.vmManagerStats[key]) === null || _a === void 0 ? void 0 : _a.availableVBrowsers) === null || _b === void 0 ? void 0 : _b.length;
                    }
                });
                yield redis.lpush('timeSeries', JSON.stringify(datapoint));
                yield redis.ltrim('timeSeries', 0, 288);
            }
            catch (e) {
                console.warn(`[TIMESERIES] %s when collecting stats`, e.code);
            }
            console.timeEnd('timeSeries');
        }
    });
}
function combine(a, b) {
    const result = Object.assign({}, a);
    Object.keys(b).forEach((key) => {
        if (key.startsWith('current')) {
            if (typeof b[key] === 'number') {
                result[key] = (result[key] || 0) + b[key];
            }
            else if (typeof b[key] === 'string') {
                result[key] = (result[key] || '') + b[key];
            }
            else if (Array.isArray(b[key])) {
                result[key] = [...(result[key] || []), ...b[key]];
            }
            else if (typeof b[key] === 'object') {
                result[key] = combine(result[key] || {}, b[key]);
            }
        }
        else {
            result[key] = a[key] || b[key];
        }
    });
    return result;
}
