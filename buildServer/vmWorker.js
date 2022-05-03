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
const utils_1 = require("./vm/utils");
const express_1 = __importDefault(require("express"));
const ioredis_1 = __importDefault(require("ioredis"));
const body_parser_1 = __importDefault(require("body-parser"));
let redis = undefined;
if (config_1.default.REDIS_URL) {
    redis = new ioredis_1.default(config_1.default.REDIS_URL);
}
const app = (0, express_1.default)();
const vmManagers = (0, utils_1.getBgVMManagers)();
const redisRefs = {};
app.use(body_parser_1.default.json());
Object.values(vmManagers).forEach((manager) => {
    manager === null || manager === void 0 ? void 0 : manager.runBackgroundJobs();
});
setInterval(() => {
    redis === null || redis === void 0 ? void 0 : redis.setex('currentVBrowserWaiting', 90, Object.keys(redisRefs).length);
}, 60000);
app.post('/assignVM', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        let redis = undefined;
        if (config_1.default.REDIS_URL) {
            redis = new ioredis_1.default(config_1.default.REDIS_URL);
            redisRefs[req.body.uid] = redis;
            setTimeout(() => {
                redis === null || redis === void 0 ? void 0 : redis.disconnect();
                delete redisRefs[req.body.uid];
            }, config_1.default.VM_ASSIGNMENT_TIMEOUT * 1000);
        }
        const pool = vmManagers[req.body.provider + (req.body.isLarge ? 'Large' : '') + req.body.region];
        if (redis && pool) {
            const vm = yield (0, utils_1.assignVM)(redis, pool);
            redis === null || redis === void 0 ? void 0 : redis.disconnect();
            delete redisRefs[req.body.uid];
            return res.json(vm !== null && vm !== void 0 ? vm : null);
        }
    }
    catch (e) {
        console.warn(e);
    }
    return res.status(400).end();
}));
app.post('/releaseVM', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    var _a;
    try {
        const pool = vmManagers[req.body.provider + (req.body.isLarge ? 'Large' : '') + req.body.region];
        (_a = redisRefs[req.body.uid]) === null || _a === void 0 ? void 0 : _a.disconnect();
        delete redisRefs[req.body.uid];
        if (req.body.id) {
            yield (pool === null || pool === void 0 ? void 0 : pool.resetVM(req.body.id));
        }
    }
    catch (e) {
        console.warn(e);
    }
    return res.end();
}));
app.get('/stats', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const vmManagerStats = {};
    for (let i = 0; i <= Object.keys(vmManagers).length; i++) {
        const key = Object.keys(vmManagers)[i];
        const vmManager = vmManagers[key];
        const availableVBrowsers = yield (redis === null || redis === void 0 ? void 0 : redis.lrange((vmManager === null || vmManager === void 0 ? void 0 : vmManager.getRedisQueueKey()) || 'availableList', 0, -1));
        const stagingVBrowsers = yield (redis === null || redis === void 0 ? void 0 : redis.lrange((vmManager === null || vmManager === void 0 ? void 0 : vmManager.getRedisStagingKey()) || 'stagingList', 0, -1));
        // const terminationVBrowsers = await redis?.smembers(
        //   vmManager?.getRedisTerminationKey() || 'terminationList',
        // );
        const size = yield (redis === null || redis === void 0 ? void 0 : redis.get((vmManager === null || vmManager === void 0 ? void 0 : vmManager.getRedisPoolSizeKey()) || 'vmPoolFull'));
        if (key && vmManager) {
            vmManagerStats[key] = {
                availableVBrowsers,
                stagingVBrowsers,
                adjustedBuffer: vmManager === null || vmManager === void 0 ? void 0 : vmManager.getAdjustedBuffer(),
                // terminationVBrowsers,
                size,
            };
        }
    }
    return res.json(vmManagerStats);
}));
app.get('/isVMPoolFull', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    var _b;
    const isVMPoolFull = {};
    for (let i = 0; i <= Object.keys(vmManagers).length; i++) {
        const key = Object.keys(vmManagers)[i];
        const value = vmManagers[key];
        if (value) {
            const availableCount = yield (redis === null || redis === void 0 ? void 0 : redis.llen(value.getRedisQueueKey()));
            const limitSize = (_b = value === null || value === void 0 ? void 0 : value.getLimitSize()) !== null && _b !== void 0 ? _b : 0;
            const currentSize = yield (redis === null || redis === void 0 ? void 0 : redis.get(value.getRedisPoolSizeKey()));
            isVMPoolFull[key] = Boolean(limitSize > 0 &&
                Number(currentSize) - Number(availableCount) > limitSize * 0.95);
        }
    }
    return res.json(isVMPoolFull);
}));
// curl -X POST http://localhost:3100/updateSnapshot -H 'Content-Type: application/json' -d '{"provider":"Hetzner","isLarge":false,"region":"US"}'
app.post('/updateSnapshot', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const pool = vmManagers[req.body.provider + (req.body.isLarge ? 'Large' : '') + req.body.region];
    const result = yield (pool === null || pool === void 0 ? void 0 : pool.updateSnapshot());
    return res.send((result === null || result === void 0 ? void 0 : result.toString()) + '\n');
}));
app.listen(config_1.default.VMWORKER_PORT, () => {
    console.log('vmWorker listening on %s', config_1.default.VMWORKER_PORT);
});
