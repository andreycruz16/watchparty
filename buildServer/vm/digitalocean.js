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
exports.DigitalOcean = void 0;
const config_1 = __importDefault(require("../config"));
const uuid_1 = require("uuid");
const axios_1 = __importDefault(require("axios"));
const base_1 = require("./base");
const DO_TOKEN = config_1.default.DO_TOKEN;
const region = 'sfo3';
const gatewayHost = config_1.default.DO_GATEWAY;
const imageId = Number(config_1.default.DO_IMAGE);
const sshKeys = config_1.default.DO_SSH_KEYS.split(',');
class DigitalOcean extends base_1.VMManager {
    constructor() {
        super(...arguments);
        this.size = 's-2vcpu-2gb'; // s-1vcpu-1gb, s-1vcpu-2gb, s-2vcpu-2gb, s-4vcpu-8gb, c-2
        this.largeSize = 's-2vcpu-2gb';
        this.minRetries = 20;
        this.id = 'DO';
        this.startVM = (name) => __awaiter(this, void 0, void 0, function* () {
            const response = yield (0, axios_1.default)({
                method: 'POST',
                url: `https://api.digitalocean.com/v2/droplets`,
                headers: {
                    Authorization: 'Bearer ' + DO_TOKEN,
                    'Content-Type': 'application/json',
                },
                data: {
                    name: name,
                    region: region,
                    size: this.isLarge ? this.largeSize : this.size,
                    image: imageId,
                    ssh_keys: sshKeys,
                    private_networking: true,
                    // user_data: cloudInit(
                    //   imageName,
                    //   this.isLarge ? '1920x1080@30' : undefined
                    // ),
                    tags: [this.getTag()],
                },
            });
            const id = response.data.droplet.id;
            return id;
        });
        this.terminateVM = (id) => __awaiter(this, void 0, void 0, function* () {
            const response = yield (0, axios_1.default)({
                method: 'DELETE',
                url: `https://api.digitalocean.com/v2/droplets/${id}`,
                headers: {
                    Authorization: 'Bearer ' + DO_TOKEN,
                    'Content-Type': 'application/json',
                },
            });
        });
        this.rebootVM = (id) => __awaiter(this, void 0, void 0, function* () {
            var _a, _b;
            // Generate a new password
            const password = (0, uuid_1.v4)();
            // Update the VM's name (also the hostname that will be used as password)
            const response = yield (0, axios_1.default)({
                method: 'POST',
                url: `https://api.digitalocean.com/v2/droplets/${id}/actions`,
                headers: {
                    Authorization: 'Bearer ' + DO_TOKEN,
                    'Content-Type': 'application/json',
                },
                data: {
                    type: 'rename',
                    name: password,
                },
            });
            const actionId = response.data.action.id;
            // Wait for the rename action to complete
            while (true) {
                const response3 = yield (0, axios_1.default)({
                    method: 'GET',
                    url: `https://api.digitalocean.com/v2/actions/${actionId}`,
                    headers: {
                        Authorization: 'Bearer ' + DO_TOKEN,
                        'Content-Type': 'application/json',
                    },
                });
                if (((_b = (_a = response3 === null || response3 === void 0 ? void 0 : response3.data) === null || _a === void 0 ? void 0 : _a.action) === null || _b === void 0 ? void 0 : _b.status) === 'completed') {
                    break;
                }
                else {
                    yield new Promise((resolve) => setTimeout(resolve, 3000));
                }
            }
            // Rebuild the VM
            const response2 = yield (0, axios_1.default)({
                method: 'POST',
                url: `https://api.digitalocean.com/v2/droplets/${id}/actions`,
                headers: {
                    Authorization: 'Bearer ' + DO_TOKEN,
                    'Content-Type': 'application/json',
                },
                data: {
                    type: 'rebuild',
                    image: imageId,
                },
            });
            // Reboot the VM
            // const response2 = await axios({
            //   method: 'POST',
            //   url: `https://api.digitalocean.com/v2/droplets/${id}/actions`,
            //   headers: {
            //     Authorization: 'Bearer ' + DO_TOKEN,
            //     'Content-Type': 'application/json',
            //   },
            //   data: {
            //     type: 'reboot',
            //   },
            // });
            return;
        });
        this.getVM = (id) => __awaiter(this, void 0, void 0, function* () {
            const response = yield (0, axios_1.default)({
                method: 'GET',
                url: `https://api.digitalocean.com/v2/droplets/${id}`,
                headers: {
                    Authorization: 'Bearer ' + DO_TOKEN,
                    'Content-Type': 'application/json',
                },
            });
            let server = this.mapServerObject(response.data.droplet);
            if (!server.private_ip) {
                return null;
            }
            return server;
        });
        this.listVMs = (filter) => __awaiter(this, void 0, void 0, function* () {
            // console.log(filter, tags);
            const response = yield (0, axios_1.default)({
                method: 'GET',
                url: `https://api.digitalocean.com/v2/droplets`,
                headers: {
                    Authorization: 'Bearer ' + DO_TOKEN,
                    'Content-Type': 'application/json',
                },
                params: {
                    // TODO need to update if over 100 results
                    per_page: 100,
                    tag_name: filter,
                },
            });
            return response.data.droplets
                .map(this.mapServerObject)
                .filter((server) => server.tags.includes(this.getTag()));
        });
        this.powerOn = (_id) => __awaiter(this, void 0, void 0, function* () { });
        this.attachToNetwork = (_id) => __awaiter(this, void 0, void 0, function* () { });
        this.updateSnapshot = () => __awaiter(this, void 0, void 0, function* () {
            return '';
        });
        this.mapServerObject = (server) => {
            var _a, _b;
            const ip = (_a = server.networks.v4.find((network) => network.type === 'private')) === null || _a === void 0 ? void 0 : _a.ip_address;
            return {
                id: (_b = server.id) === null || _b === void 0 ? void 0 : _b.toString(),
                pass: server.name,
                // The gateway handles SSL termination and proxies to the private IP
                host: `${gatewayHost}/?ip=${ip}`,
                private_ip: ip,
                state: server.status,
                tags: server.tags,
                creation_date: server.created_at,
                provider: this.id,
                large: this.isLarge,
                region: this.region,
            };
        };
    }
}
exports.DigitalOcean = DigitalOcean;
