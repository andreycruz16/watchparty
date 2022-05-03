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
exports.getSessionLimitSeconds = exports.getBgVMManagers = exports.assignVM = exports.imageName = void 0;
const config_1 = __importDefault(require("../config"));
const scaleway_1 = require("./scaleway");
const hetzner_1 = require("./hetzner");
const digitalocean_1 = require("./digitalocean");
const docker_1 = require("./docker");
exports.imageName = 'howardc93/vbrowser';
const assignVM = (redis, vmManager) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const assignStart = Number(new Date());
        let selected = null;
        while (!selected) {
            if (vmManager.getMinSize() === 0) {
                // This code spawns a VM if none is available in the pool
                const availableCount = yield redis.llen(vmManager.getRedisQueueKey());
                if (!availableCount) {
                    yield vmManager.startVMWrapper();
                }
            }
            let resp = yield redis.blpop(vmManager.getRedisQueueKey(), config_1.default.VM_ASSIGNMENT_TIMEOUT);
            if (!resp) {
                return undefined;
            }
            const id = resp[1];
            console.log('[ASSIGN]', id);
            const lock = yield redis.set('lock:' + vmManager.id + ':' + id, '1', 'NX', 'EX', 300);
            if (!lock) {
                console.log('failed to acquire lock on VM:', id);
                continue;
            }
            const cachedData = yield redis.get(vmManager.getRedisHostCacheKey() + ':' + id);
            let candidate = cachedData && cachedData.startsWith('{') && JSON.parse(cachedData);
            if (!candidate) {
                candidate = yield vmManager.getVM(id);
            }
            selected = candidate;
        }
        const assignEnd = Number(new Date());
        const assignElapsed = assignEnd - assignStart;
        yield redis.lpush('vBrowserStartMS', assignElapsed);
        yield redis.ltrim('vBrowserStartMS', 0, 24);
        console.log('[ASSIGN]', selected.id, assignElapsed + 'ms');
        const retVal = Object.assign(Object.assign({}, selected), { assignTime: Number(new Date()) });
        return retVal;
    }
    catch (e) {
        console.warn(e);
        return undefined;
    }
});
exports.assignVM = assignVM;
function getVMManager({ provider, isLarge, region, limitSize, minSize, }) {
    let vmManager = null;
    if (config_1.default.REDIS_URL &&
        config_1.default.SCW_SECRET_KEY &&
        config_1.default.SCW_ORGANIZATION_ID &&
        provider === 'Scaleway') {
        vmManager = new scaleway_1.Scaleway(isLarge, region, limitSize, minSize);
    }
    else if (config_1.default.REDIS_URL &&
        config_1.default.HETZNER_TOKEN &&
        provider === 'Hetzner') {
        vmManager = new hetzner_1.Hetzner(isLarge, region, limitSize, minSize);
    }
    else if (config_1.default.REDIS_URL && config_1.default.DO_TOKEN && provider === 'DO') {
        vmManager = new digitalocean_1.DigitalOcean(isLarge, region, limitSize, minSize);
    }
    else if (config_1.default.REDIS_URL &&
        config_1.default.DOCKER_VM_HOST &&
        provider === 'Docker') {
        vmManager = new docker_1.Docker(isLarge, region, limitSize, minSize);
    }
    return vmManager;
}
function getBgVMManagers() {
    const conf = [
        {
            provider: 'Hetzner',
            isLarge: true,
            region: 'US',
            limitSize: Number(config_1.default.HETZNER_POOL_SIZE_LARGE.split(',')[1]),
            minSize: Number(config_1.default.HETZNER_POOL_SIZE_LARGE.split(',')[0]),
        },
        {
            provider: 'Hetzner',
            isLarge: false,
            region: 'US',
            limitSize: Number(config_1.default.HETZNER_POOL_SIZE.split(',')[1]),
            minSize: Number(config_1.default.HETZNER_POOL_SIZE.split(',')[0]),
        },
        {
            provider: 'Hetzner',
            isLarge: true,
            region: 'EU',
            limitSize: 0,
            minSize: 0,
        },
        {
            provider: 'Hetzner',
            isLarge: false,
            region: 'EU',
            limitSize: 0,
            minSize: 0,
        },
        {
            provider: 'Scaleway',
            isLarge: true,
            region: 'EU',
            limitSize: 0,
            minSize: 0,
        },
        {
            provider: 'Scaleway',
            isLarge: false,
            region: 'EU',
            limitSize: 0,
            minSize: 0,
        },
        {
            provider: 'DO',
            isLarge: true,
            region: 'US',
            limitSize: 0,
            minSize: 0,
        },
        {
            provider: 'DO',
            isLarge: false,
            region: 'US',
            limitSize: 0,
            minSize: 0,
        },
        {
            provider: 'Docker',
            isLarge: true,
            region: 'US',
            limitSize: 0,
            minSize: 0,
        },
        {
            provider: 'Docker',
            isLarge: false,
            region: 'US',
            limitSize: 0,
            minSize: 0,
        },
    ];
    const result = {};
    conf.forEach((c) => {
        result[c.provider + (c.isLarge ? 'Large' : '') + c.region] =
            getVMManager(c);
    });
    return result;
}
exports.getBgVMManagers = getBgVMManagers;
function getSessionLimitSeconds(isLarge) {
    return isLarge
        ? config_1.default.VBROWSER_SESSION_SECONDS_LARGE
        : config_1.default.VBROWSER_SESSION_SECONDS;
}
exports.getSessionLimitSeconds = getSessionLimitSeconds;
