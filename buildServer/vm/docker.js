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
exports.Docker = void 0;
// This assumes an installation of Docker exists at the Docker VM host
// and that host is configured to accept our SSH key
const config_1 = __importDefault(require("../config"));
const base_1 = require("./base");
const utils_1 = require("./utils");
//@ts-ignore
const ssh_exec_1 = __importDefault(require("ssh-exec"));
const gatewayHost = config_1.default.DOCKER_VM_HOST;
const sshConfig = {
    user: config_1.default.DOCKER_VM_HOST_SSH_USER || 'root',
    host: gatewayHost,
    // Defaults to ~/.ssh/id_rsa
    key: config_1.default.DOCKER_VM_HOST_SSH_KEY_BASE64
        ? Buffer.from(config_1.default.DOCKER_VM_HOST_SSH_KEY_BASE64, 'base64')
        : undefined,
};
class Docker extends base_1.VMManager {
    constructor() {
        super(...arguments);
        this.size = '';
        this.largeSize = '';
        this.minRetries = 0;
        this.id = 'Docker';
        this.startVM = (name) => __awaiter(this, void 0, void 0, function* () {
            return new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () {
                (0, ssh_exec_1.default)(`
        #!/bin/bash
        set -e
        PORT=$(comm -23 <(seq 5000 5063 | sort) <(ss -Htan | awk '{print $4}' | cut -d':' -f2 | sort -u) | shuf | head -n 1)
        INDEX=$(($PORT - 5000))
        UDP_START=$((59000+$INDEX*100))
        UDP_END=$((59099+$INDEX*100))
        docker run -d --rm --name=${name} --net=host --memory="2g" --cpus="2" -v /etc/letsencrypt:/etc/letsencrypt -l ${this.getTag()} -l index=$INDEX --log-opt max-size=1g --shm-size=1g --cap-add="SYS_ADMIN" -e NEKO_KEY="/etc/letsencrypt/live/${gatewayHost}/privkey.pem" -e NEKO_CERT="/etc/letsencrypt/live/${gatewayHost}/fullchain.pem" -e DISPLAY=":99.0" -e NEKO_SCREEN="${this.isLarge ? '1920x1080@30' : ''}" -e NEKO_PASSWORD=${name} -e NEKO_PASSWORD_ADMIN=${name} -e NEKO_BIND=":$PORT" -e NEKO_EPR=":$UDP_START-$UDP_END" -e NEKO_H264="1" ${utils_1.imageName}
        `, sshConfig, (err, stdout) => {
                    if (err) {
                        return reject(err);
                    }
                    console.log(stdout);
                    resolve(stdout.trim());
                });
            }));
        });
        this.terminateVM = (id) => __awaiter(this, void 0, void 0, function* () {
            return new Promise((resolve, reject) => {
                (0, ssh_exec_1.default)(`docker rm -f ${id}`, sshConfig, (err, stdout) => {
                    if (err) {
                        return reject(err);
                    }
                    resolve();
                });
            });
        });
        this.rebootVM = (id) => __awaiter(this, void 0, void 0, function* () {
            return yield this.terminateVM(id);
        });
        // Override the base method, since we don't need to reuse docker containers
        this.resetVM = (id) => __awaiter(this, void 0, void 0, function* () {
            return yield this.terminateVM(id);
        });
        this.getVM = (id) => __awaiter(this, void 0, void 0, function* () {
            return new Promise((resolve, reject) => {
                (0, ssh_exec_1.default)(`docker inspect ${id}`, sshConfig, (err, stdout) => {
                    if (err) {
                        return reject(err);
                    }
                    let data = null;
                    try {
                        data = JSON.parse(stdout)[0];
                        if (!data) {
                            return reject(new Error('no container with this ID found'));
                        }
                    }
                    catch (_a) {
                        console.warn(stdout);
                        return reject('failed to parse json');
                    }
                    let server = this.mapServerObject(data);
                    return resolve(server);
                });
            });
        });
        this.listVMs = (filter) => __awaiter(this, void 0, void 0, function* () {
            return new Promise((resolve, reject) => {
                (0, ssh_exec_1.default)(`docker inspect $(docker ps --filter label=${filter} --quiet --no-trunc) || true`, sshConfig, (err, stdout) => {
                    // Swallow exceptions and return empty array
                    if (err) {
                        return resolve([]);
                    }
                    if (!stdout) {
                        return resolve([]);
                    }
                    let data = [];
                    try {
                        data = JSON.parse(stdout);
                    }
                    catch (e) {
                        console.warn(stdout);
                        return reject('failed to parse json');
                    }
                    return resolve(data.map(this.mapServerObject));
                });
            });
        });
        this.powerOn = (id) => __awaiter(this, void 0, void 0, function* () { });
        this.attachToNetwork = (id) => __awaiter(this, void 0, void 0, function* () { });
        this.updateSnapshot = () => __awaiter(this, void 0, void 0, function* () {
            return '';
        });
        this.mapServerObject = (server) => {
            var _a, _b, _c, _d, _e, _f;
            return ({
                id: server.Id,
                pass: (_a = server.Name) === null || _a === void 0 ? void 0 : _a.slice(1),
                host: `${gatewayHost}:${5000 + Number((_c = (_b = server.Config) === null || _b === void 0 ? void 0 : _b.Labels) === null || _c === void 0 ? void 0 : _c.index)}`,
                private_ip: '',
                state: (_d = server.State) === null || _d === void 0 ? void 0 : _d.Status,
                tags: (_e = server.Config) === null || _e === void 0 ? void 0 : _e.Labels,
                creation_date: (_f = server.State) === null || _f === void 0 ? void 0 : _f.StartedAt,
                provider: this.id,
                large: this.isLarge,
                region: this.region,
            });
        };
    }
}
exports.Docker = Docker;
