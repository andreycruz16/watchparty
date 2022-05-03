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
var __asyncValues = (this && this.__asyncValues) || function (o) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var m = o[Symbol.asyncIterator], i;
    return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
    function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
    function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getAllActiveSubscriptions = exports.getAllCustomers = exports.createSelfServicePortal = exports.getCustomerByEmail = void 0;
const config_1 = __importDefault(require("../config"));
const stripe_1 = __importDefault(require("stripe"));
const stripe = new stripe_1.default(config_1.default.STRIPE_SECRET_KEY, {
    apiVersion: '2020-08-27',
});
function getCustomerByEmail(email) {
    return __awaiter(this, void 0, void 0, function* () {
        if (!config_1.default.STRIPE_SECRET_KEY) {
            return undefined;
        }
        const customer = yield stripe.customers.list({
            email,
            expand: ['data.subscriptions'],
        });
        return customer === null || customer === void 0 ? void 0 : customer.data[0];
    });
}
exports.getCustomerByEmail = getCustomerByEmail;
function createSelfServicePortal(customerId, returnUrl) {
    return __awaiter(this, void 0, void 0, function* () {
        return yield stripe.billingPortal.sessions.create({
            customer: customerId,
            return_url: returnUrl,
        });
    });
}
exports.createSelfServicePortal = createSelfServicePortal;
function getAllCustomers() {
    var e_1, _a;
    return __awaiter(this, void 0, void 0, function* () {
        const result = [];
        try {
            for (var _b = __asyncValues(stripe.customers.list({ limit: 100 })), _c; _c = yield _b.next(), !_c.done;) {
                const customer = _c.value;
                result.push(customer);
            }
        }
        catch (e_1_1) { e_1 = { error: e_1_1 }; }
        finally {
            try {
                if (_c && !_c.done && (_a = _b.return)) yield _a.call(_b);
            }
            finally { if (e_1) throw e_1.error; }
        }
        return result;
    });
}
exports.getAllCustomers = getAllCustomers;
function getAllActiveSubscriptions() {
    var e_2, _a;
    return __awaiter(this, void 0, void 0, function* () {
        const result = [];
        try {
            for (var _b = __asyncValues(stripe.subscriptions.list({
                limit: 100,
                status: 'active',
            })), _c; _c = yield _b.next(), !_c.done;) {
                const sub = _c.value;
                result.push(sub);
            }
        }
        catch (e_2_1) { e_2 = { error: e_2_1 }; }
        finally {
            try {
                if (_c && !_c.done && (_a = _b.return)) yield _a.call(_b);
            }
            finally { if (e_2) throw e_2.error; }
        }
        return result;
    });
}
exports.getAllActiveSubscriptions = getAllActiveSubscriptions;
