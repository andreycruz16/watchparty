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
exports.Scaleway = void 0;
const config_1 = __importDefault(require("../config"));
const uuid_1 = require("uuid");
const axios_1 = __importDefault(require("axios"));
const base_1 = require("./base");
const SCW_SECRET_KEY = config_1.default.SCW_SECRET_KEY;
const SCW_ORGANIZATION_ID = config_1.default.SCW_ORGANIZATION_ID;
const region = 'nl-ams-1'; //fr-par-1
const gatewayHost = config_1.default.SCW_GATEWAY;
const imageId = config_1.default.SCW_IMAGE;
class Scaleway extends base_1.VMManager {
    constructor() {
        super(...arguments);
        this.size = 'DEV1-S'; // DEV1-S, DEV1-M, DEV1-L, GP1-XS
        this.largeSize = 'DEV1-M';
        this.minRetries = 20;
        this.id = 'Scaleway';
        this.startVM = (name) => __awaiter(this, void 0, void 0, function* () {
            const response = yield (0, axios_1.default)({
                method: 'POST',
                url: `https://api.scaleway.com/instance/v1/zones/${region}/servers`,
                headers: {
                    'X-Auth-Token': SCW_SECRET_KEY,
                    'Content-Type': 'application/json',
                },
                data: {
                    name: name,
                    dynamic_ip_required: true,
                    commercial_type: this.isLarge ? this.largeSize : this.size,
                    image: imageId,
                    volumes: {},
                    organization: SCW_ORGANIZATION_ID,
                    tags: [this.getTag()],
                },
            });
            // console.log(response.data);
            const id = response.data.server.id;
            const response2 = yield (0, axios_1.default)({
                method: 'PATCH',
                url: `https://api.scaleway.com/instance/v1/zones/${region}/servers/${id}/user_data/cloud-init`,
                headers: {
                    'X-Auth-Token': SCW_SECRET_KEY,
                    'Content-Type': 'text/plain',
                },
                // set userdata for boot action
                //data: cloudInit(imageName, this.isLarge ? '1920x1080@30' : undefined),
            });
            // console.log(response2.data);
            // boot the instance
            const response3 = yield (0, axios_1.default)({
                method: 'POST',
                url: `https://api.scaleway.com/instance/v1/zones/${region}/servers/${id}/action`,
                headers: {
                    'X-Auth-Token': SCW_SECRET_KEY,
                    'Content-Type': 'application/json',
                },
                data: {
                    action: 'poweron',
                },
            });
            // console.log(response3.data);
            return id;
        });
        this.terminateVM = (id) => __awaiter(this, void 0, void 0, function* () {
            const response = yield (0, axios_1.default)({
                method: 'POST',
                url: `https://api.scaleway.com/instance/v1/zones/${region}/servers/${id}/action`,
                headers: {
                    'X-Auth-Token': SCW_SECRET_KEY,
                    'Content-Type': 'application/json',
                },
                data: {
                    action: 'terminate',
                },
            });
        });
        this.rebootVM = (id) => __awaiter(this, void 0, void 0, function* () {
            // Generate a new password
            const password = (0, uuid_1.v4)();
            // Update the VM's name (also the hostname that will be used as password)
            const response = yield (0, axios_1.default)({
                method: 'PATCH',
                url: `https://api.scaleway.com/instance/v1/zones/${region}/servers/${id}`,
                headers: {
                    'X-Auth-Token': SCW_SECRET_KEY,
                    'Content-Type': 'application/json',
                },
                data: {
                    name: password,
                    tags: [this.getTag()],
                },
            });
            // Reboot the VM (also destroys the Docker container since it has --rm flag)
            const response2 = yield (0, axios_1.default)({
                method: 'POST',
                url: `https://api.scaleway.com/instance/v1/zones/${region}/servers/${id}/action`,
                headers: {
                    'X-Auth-Token': SCW_SECRET_KEY,
                    'Content-Type': 'application/json',
                },
                data: {
                    action: 'reboot',
                },
            });
            return;
        });
        this.getVM = (id) => __awaiter(this, void 0, void 0, function* () {
            const response = yield (0, axios_1.default)({
                method: 'GET',
                url: `https://api.scaleway.com/instance/v1/zones/${region}/servers/${id}`,
                headers: {
                    'X-Auth-Token': SCW_SECRET_KEY,
                    'Content-Type': 'application/json',
                },
            });
            let server = this.mapServerObject(response.data.server);
            if (!server.private_ip) {
                return null;
            }
            return server;
        });
        this.listVMs = (filter) => __awaiter(this, void 0, void 0, function* () {
            const mapping = {
                available: 'available',
                inUse: 'inUse',
            };
            let tags = mapping[filter];
            // console.log(filter, tags);
            const response = yield (0, axios_1.default)({
                method: 'GET',
                url: `https://api.scaleway.com/instance/v1/zones/${region}/servers`,
                headers: {
                    'X-Auth-Token': SCW_SECRET_KEY,
                    'Content-Type': 'application/json',
                },
                params: {
                    // TODO need to update if over 100 results
                    per_page: 100,
                    tags,
                },
            });
            return response.data.servers
                .map(this.mapServerObject)
                .filter((server) => server.tags.includes(this.getTag()));
        });
        this.powerOn = (_id) => __awaiter(this, void 0, void 0, function* () { });
        this.attachToNetwork = (_id) => __awaiter(this, void 0, void 0, function* () { });
        this.updateSnapshot = () => __awaiter(this, void 0, void 0, function* () {
            return '';
        });
        this.mapServerObject = (server) => ({
            id: server.id,
            pass: server.name,
            // The gateway handles SSL termination and proxies to the private IP
            host: `${gatewayHost}/?ip=${server.private_ip}`,
            private_ip: server.private_ip,
            state: server.state,
            tags: server.tags,
            creation_date: server.creation_date,
            provider: this.id,
            large: this.isLarge,
            region: this.region,
        });
    }
}
exports.Scaleway = Scaleway;
