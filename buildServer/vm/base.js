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
exports.VMManager = void 0;
const config_1 = __importDefault(require("../config"));
const ioredis_1 = __importDefault(require("ioredis"));
const axios_1 = __importDefault(require("axios"));
const uuid_1 = require("uuid");
const redis_1 = require("../utils/redis");
let redis = undefined;
if (config_1.default.REDIS_URL) {
    redis = new ioredis_1.default(config_1.default.REDIS_URL);
}
const incrInterval = 5 * 1000;
const decrInterval = 1 * 60 * 1000;
const cleanupInterval = 5 * 60 * 1000;
const updateSizeInterval = 60 * 1000;
class VMManager {
    constructor(large, region, limitSize, minSize) {
        this.isLarge = false;
        this.region = '';
        this.currentSize = 0;
        this.limitSize = 0;
        this.minSize = 0;
        this.getMinSize = () => {
            return this.minSize;
        };
        this.getLimitSize = () => {
            return this.limitSize;
        };
        this.getMinBuffer = () => {
            return this.limitSize * 0.05;
        };
        this.getCurrentSize = () => {
            return this.currentSize;
        };
        this.getAdjustedBuffer = () => {
            let minBuffer = this.getMinBuffer();
            // If ramping config, adjust minBuffer based on the hour
            // During ramp down hours, keep a smaller buffer
            // During ramp up hours, keep a larger buffer
            const rampDownHours = config_1.default.VM_POOL_RAMP_DOWN_HOURS.split(',').map(Number);
            const rampUpHours = config_1.default.VM_POOL_RAMP_UP_HOURS.split(',').map(Number);
            const nowHour = new Date().getUTCHours();
            const isRampDown = rampDownHours.length &&
                pointInInterval24(nowHour, rampDownHours[0], rampDownHours[1]);
            const isRampUp = rampUpHours.length &&
                pointInInterval24(nowHour, rampUpHours[0], rampUpHours[1]);
            if (isRampDown) {
                minBuffer /= 2;
            }
            else if (isRampUp) {
                minBuffer *= 1.5;
            }
            return [Math.ceil(minBuffer), Math.ceil(minBuffer * 1.5)];
        };
        this.getRedisQueueKey = () => {
            return ('availableList' + this.id + this.region + (this.isLarge ? 'Large' : ''));
        };
        this.getRedisStagingKey = () => {
            return ('stagingList' + this.id + this.region + (this.isLarge ? 'Large' : ''));
        };
        this.getRedisAllKey = () => {
            return 'allList' + this.id + this.region + (this.isLarge ? 'Large' : '');
        };
        this.getRedisHostCacheKey = () => {
            return 'hostCache' + this.id + this.region + (this.isLarge ? 'Large' : '');
        };
        this.getRedisPoolSizeKey = () => {
            return 'vmPoolFull' + this.id + this.region + (this.isLarge ? 'Large' : '');
        };
        this.getRedisTerminationKey = () => {
            return ('terminationList' + this.id + this.region + (this.isLarge ? 'Large' : ''));
        };
        this.getTag = () => {
            return ((config_1.default.VBROWSER_TAG || 'vbrowser') +
                this.region +
                (this.isLarge ? 'Large' : ''));
        };
        this.resetVM = (id) => __awaiter(this, void 0, void 0, function* () {
            // We can attempt to reuse the instance which is more efficient if users tend to use them for a short time
            // Otherwise terminating them is simpler but more expensive since they're billed for an hour
            console.log('[RESET]', id);
            yield this.rebootVM(id);
            // Delete any locks/caches
            yield this.redis.del('lock:' + this.id + ':' + id);
            yield this.redis.del(this.getRedisHostCacheKey() + ':' + id);
            // Add the VM back to the pool
            yield this.redis.rpush(this.getRedisStagingKey(), id);
        });
        this.startVMWrapper = () => __awaiter(this, void 0, void 0, function* () {
            var _a, _b, _c, _d;
            // generate credentials and boot a VM
            try {
                const password = (0, uuid_1.v4)();
                const id = yield this.startVM(password);
                yield this.redis.rpush(this.getRedisStagingKey(), id);
                (0, redis_1.redisCount)('vBrowserLaunches');
                return id;
            }
            catch (e) {
                console.log((_a = e.response) === null || _a === void 0 ? void 0 : _a.status, JSON.stringify((_b = e.response) === null || _b === void 0 ? void 0 : _b.data), (_c = e.config) === null || _c === void 0 ? void 0 : _c.url, (_d = e.config) === null || _d === void 0 ? void 0 : _d.data);
            }
        });
        this.terminateVMWrapper = (id) => __awaiter(this, void 0, void 0, function* () {
            console.log('[TERMINATE]', id);
            // Remove from lists, if it exists
            yield this.redis.lrem(this.getRedisQueueKey(), 0, id);
            yield this.redis.lrem(this.getRedisStagingKey(), 0, id);
            // Get the VM data to calculate lifetime, if we fail do the terminate anyway
            // const lifetime = await this.terminateVMMetrics(id);
            yield this.terminateVM(id);
            // if (lifetime) {
            //   await this.redis.lpush('vBrowserVMLifetime', lifetime);
            //   await this.redis.ltrim('vBrowserVMLifetime', 0, 24);
            // }
            // Delete any locks
            yield this.redis.del('lock:' + this.id + ':' + id);
            yield this.redis.del(this.getRedisHostCacheKey() + ':' + id);
        });
        this.terminateVMMetrics = (id) => __awaiter(this, void 0, void 0, function* () {
            var _e;
            try {
                const vm = yield this.getVM(id);
                if (vm) {
                    const lifetime = Number(new Date()) - Number(new Date(vm.creation_date));
                    return lifetime;
                }
            }
            catch (e) {
                console.warn((_e = e.response) === null || _e === void 0 ? void 0 : _e.data);
            }
            return 0;
        });
        this.runBackgroundJobs = () => __awaiter(this, void 0, void 0, function* () {
            console.log('[VMWORKER] starting background jobs for %s', this.getRedisQueueKey());
            const resizeVMGroupIncr = () => __awaiter(this, void 0, void 0, function* () {
                const availableCount = yield this.redis.llen(this.getRedisQueueKey());
                const stagingCount = yield this.redis.llen(this.getRedisStagingKey());
                let launch = false;
                launch =
                    availableCount + stagingCount < this.getAdjustedBuffer()[0] &&
                        this.getCurrentSize() != null &&
                        this.getCurrentSize() < (this.getLimitSize() || Infinity);
                if (launch) {
                    console.log('[RESIZE-LAUNCH]', 'minimum:', this.getAdjustedBuffer()[0], 'available:', availableCount, 'staging:', stagingCount, 'currentSize:', this.getCurrentSize(), 'limit:', this.getLimitSize());
                    this.startVMWrapper();
                }
            });
            const resizeVMGroupDecr = () => __awaiter(this, void 0, void 0, function* () {
                let unlaunch = false;
                const availableCount = yield this.redis.llen(this.getRedisQueueKey());
                unlaunch = availableCount > this.getAdjustedBuffer()[1];
                if (unlaunch) {
                    const ids = yield this.redis.smembers(this.getRedisTerminationKey());
                    // Remove the first available VM
                    let first = null;
                    let rem = 0;
                    while (ids.length && !rem) {
                        first = ids.shift();
                        rem = first
                            ? yield this.redis.lrem(this.getRedisQueueKey(), 1, first)
                            : 0;
                        if (first && rem) {
                            console.log('[RESIZE-UNLAUNCH]', first);
                            yield this.terminateVMWrapper(first);
                        }
                    }
                }
            });
            const updateSize = () => __awaiter(this, void 0, void 0, function* () {
                const allVMs = yield this.listVMs(this.getTag());
                const now = Date.now();
                this.currentSize = allVMs.length;
                yield this.redis.setex(this.getRedisPoolSizeKey(), 2 * 60, allVMs.length);
                let sortedVMs = allVMs
                    // Sort newest first (decreasing alphabetically)
                    .sort((a, b) => { var _a; return -((_a = a.creation_date) === null || _a === void 0 ? void 0 : _a.localeCompare(b.creation_date)); })
                    // Remove the minimum number of VMs to keep
                    .slice(0, -this.getMinSize() || undefined)
                    // Consider only VMs that have been up for most of an hour
                    .filter((vm) => (now - Number(new Date(vm.creation_date))) % (60 * 60 * 1000) >
                    config_1.default.VM_MIN_UPTIME_MINUTES * 60 * 1000);
                const cmd = this.redis.multi().del(this.getRedisTerminationKey());
                if (sortedVMs.length) {
                    cmd.sadd(this.getRedisTerminationKey(), sortedVMs.map((vm) => vm.id));
                }
                yield cmd.exec();
                if (allVMs.length) {
                    const availableKeys = yield this.redis.lrange(this.getRedisQueueKey(), 0, -1);
                    const stagingKeys = yield this.redis.lrange(this.getRedisStagingKey(), 0, -1);
                    console.log('[STATS] %s: currentSize %s, available %s, staging %s, buffer %s', this.getRedisQueueKey(), allVMs.length, availableKeys.length, stagingKeys.length, this.getAdjustedBuffer());
                }
            });
            const cleanupVMGroup = () => __awaiter(this, void 0, void 0, function* () {
                var _f;
                // Clean up hanging VMs
                // It's possible we created a VM but lost track of it in redis
                // Take the list of VMs from API, subtract VMs that have a lock in redis or are in the available or staging pool, delete the rest
                const allVMs = yield this.listVMs(this.getTag());
                const usedKeys = [];
                for (let i = 0; i < allVMs.length; i++) {
                    if (yield this.redis.get(`lock:${this.id}:${allVMs[i].id}`)) {
                        usedKeys.push(allVMs[i].id);
                    }
                }
                const availableKeys = yield this.redis.lrange(this.getRedisQueueKey(), 0, -1);
                const stagingKeys = yield this.redis.lrange(this.getRedisStagingKey(), 0, -1);
                const dontDelete = new Set([
                    ...usedKeys,
                    ...availableKeys,
                    ...stagingKeys,
                ]);
                console.log('[CLEANUP] %s: cleanup %s VMs', this.getRedisQueueKey(), allVMs.length - dontDelete.size);
                for (let i = 0; i < allVMs.length; i++) {
                    const server = allVMs[i];
                    if (!dontDelete.has(server.id)) {
                        console.log('[CLEANUP]', server.id);
                        try {
                            yield this.resetVM(server.id);
                        }
                        catch (e) {
                            console.warn((_f = e.response) === null || _f === void 0 ? void 0 : _f.data);
                        }
                        //this.terminateVMWrapper(server.id);
                        yield new Promise((resolve) => setTimeout(resolve, 2000));
                    }
                }
            });
            const checkStaging = () => __awaiter(this, void 0, void 0, function* () {
                try {
                    // const checkStagingStart = this.isLarge
                    //   ? 0
                    //   : Math.floor(Date.now() / 1000);
                    // if (checkStagingStart) {
                    //   console.log('[CHECKSTAGING]', checkStagingStart);
                    // }
                    // Loop through staging list and check if VM is ready
                    const stagingKeys = yield this.redis.lrange(this.getRedisStagingKey(), 0, -1);
                    const stagingPromises = stagingKeys.map((id) => {
                        return new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
                            var _a, _b, _c;
                            const retryCount = yield this.redis.incr(this.getRedisStagingKey() + ':' + id);
                            if (retryCount < this.minRetries) {
                                // Do a minimum # of retries to give reboot time
                                return resolve(id + ', ' + retryCount + ', ' + false);
                            }
                            let ready = false;
                            let vm = null;
                            try {
                                const cached = yield this.redis.get(this.getRedisHostCacheKey() + ':' + id);
                                vm = cached ? JSON.parse(cached) : null;
                                if (!vm &&
                                    (retryCount === this.minRetries + 1 || retryCount % 20 === 0)) {
                                    vm = yield this.getVM(id);
                                    if (vm === null || vm === void 0 ? void 0 : vm.host) {
                                        console.log('[CHECKSTAGING] caching host %s for id %s', vm === null || vm === void 0 ? void 0 : vm.host, id);
                                        yield this.redis.setex(this.getRedisHostCacheKey() + ':' + id, 2 * 3600, JSON.stringify(vm));
                                    }
                                }
                            }
                            catch (e) {
                                console.warn((_a = e.response) === null || _a === void 0 ? void 0 : _a.data);
                                if (((_b = e.response) === null || _b === void 0 ? void 0 : _b.status) === 404) {
                                    yield this.redis.lrem(this.getRedisQueueKey(), 0, id);
                                    yield this.redis.lrem(this.getRedisStagingKey(), 0, id);
                                    yield this.redis.del(this.getRedisStagingKey() + ':' + id);
                                    return reject();
                                }
                            }
                            ready = yield checkVMReady((_c = vm === null || vm === void 0 ? void 0 : vm.host) !== null && _c !== void 0 ? _c : '');
                            //ready = retryCount > 100;
                            if (ready) {
                                console.log('[CHECKSTAGING] ready:', id, vm === null || vm === void 0 ? void 0 : vm.host, retryCount);
                                // If it is, move it to available list
                                const rem = yield this.redis.lrem(this.getRedisStagingKey(), 1, id);
                                if (rem) {
                                    yield this.redis
                                        .multi()
                                        .rpush(this.getRedisQueueKey(), id)
                                        .del(this.getRedisStagingKey() + ':' + id)
                                        .exec();
                                    yield this.redis.lpush('vBrowserStageRetries', retryCount);
                                    yield this.redis.ltrim('vBrowserStageRetries', 0, 24);
                                }
                            }
                            else {
                                if (retryCount >= 240) {
                                    console.log('[CHECKSTAGING]', 'giving up:', id);
                                    yield this.redis
                                        .multi()
                                        .lrem(this.getRedisStagingKey(), 0, id)
                                        .del(this.getRedisStagingKey() + ':' + id)
                                        .exec();
                                    (0, redis_1.redisCount)('vBrowserStagingFails');
                                    yield this.redis.lpush('vBrowserStageFails', id);
                                    yield this.redis.ltrim('vBrowserStageFails', 0, 24);
                                    yield this.resetVM(id);
                                    //await this.terminateVMWrapper(id);
                                }
                                else {
                                    if (retryCount % 150 === 0) {
                                        console.log('[CHECKSTAGING] %s attempt to poweron, attach to network', id);
                                        this.powerOn(id);
                                        //this.attachToNetwork(id);
                                    }
                                    if (retryCount % 30 === 0) {
                                        console.log('[CHECKSTAGING]', 'not ready:', id, vm === null || vm === void 0 ? void 0 : vm.host, retryCount);
                                    }
                                }
                            }
                            resolve(id + ', ' + retryCount + ', ' + ready);
                        }));
                    });
                    const result = yield Promise.race([
                        Promise.allSettled(stagingPromises),
                        new Promise((resolve) => setTimeout(resolve, 30000)),
                    ]);
                    // if (checkStagingStart) {
                    //   console.log('[CHECKSTAGING-DONE]', checkStagingStart, result);
                    // }
                    return result;
                }
                catch (e) {
                    console.warn('[CHECKSTAGING-ERROR]', e);
                    return [];
                }
            });
            setInterval(resizeVMGroupIncr, incrInterval);
            setInterval(resizeVMGroupDecr, decrInterval);
            updateSize();
            setInterval(updateSize, updateSizeInterval);
            setImmediate(() => __awaiter(this, void 0, void 0, function* () {
                var _g;
                while (true) {
                    try {
                        yield cleanupVMGroup();
                    }
                    catch (e) {
                        console.warn((_g = e.response) === null || _g === void 0 ? void 0 : _g.data);
                    }
                    yield new Promise((resolve) => setTimeout(resolve, cleanupInterval));
                }
            }));
            const checkStagingInterval = 1000;
            while (true) {
                yield new Promise((resolve) => setTimeout(resolve, checkStagingInterval));
                yield checkStaging();
            }
        });
        this.isLarge = Boolean(large);
        this.region = region;
        this.limitSize = Number(limitSize);
        this.minSize = Number(minSize);
        if (!redis) {
            throw new Error('Cannot construct VMManager without Redis');
        }
        this.redis = redis;
    }
}
exports.VMManager = VMManager;
function checkVMReady(host) {
    return __awaiter(this, void 0, void 0, function* () {
        const url = 'https://' + host.replace('/', '/health');
        try {
            // const out = execSync(`curl -i -L -v --ipv4 '${host}'`);
            // if (!out.toString().startsWith('OK') && !out.toString().startsWith('404 page not found')) {
            //   throw new Error('mismatched response from health');
            // }
            yield (0, axios_1.default)({
                method: 'GET',
                url,
                timeout: 1000,
            });
        }
        catch (e) {
            // console.log(url, e.message, e.response?.status);
            return false;
        }
        return true;
    });
}
function pointInInterval24(x, a, b) {
    return nonNegativeMod(x - a, 24) <= nonNegativeMod(b - a, 24);
}
function nonNegativeMod(n, m) {
    return ((n % m) + m) % m;
}
