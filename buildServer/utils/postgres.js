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
Object.defineProperty(exports, "__esModule", { value: true });
exports.upsertObject = exports.insertObject = exports.updateObject = void 0;
function updateObject(postgres, table, object, condition) {
    return __awaiter(this, void 0, void 0, function* () {
        const columns = Object.keys(object);
        const values = Object.values(object);
        // TODO support compound conditions, not just one
        let query = `UPDATE ${table} SET ${columns
            .map((c, i) => `"${c}" = $${i + 1}`)
            .join(',')}
    WHERE "${Object.keys(condition)[0]}" = $${Object.keys(object).length + 1}
    RETURNING *`;
        //console.log(query);
        const result = yield postgres.query(query, [
            ...values,
            condition[Object.keys(condition)[0]],
        ]);
        return result;
    });
}
exports.updateObject = updateObject;
function insertObject(postgres, table, object) {
    return __awaiter(this, void 0, void 0, function* () {
        const columns = Object.keys(object);
        const values = Object.values(object);
        let query = `INSERT INTO ${table} (${columns.map((c) => `"${c}"`).join(',')})
    VALUES (${values.map((_, i) => '$' + (i + 1)).join(',')})
    RETURNING *`;
        // console.log(query);
        const result = yield postgres.query(query, values);
        return result;
    });
}
exports.insertObject = insertObject;
function upsertObject(postgres, table, object, conflict) {
    return __awaiter(this, void 0, void 0, function* () {
        const columns = Object.keys(object);
        const values = Object.values(object);
        let query = `INSERT INTO ${table} (${columns.map((c) => `"${c}"`).join(',')})
    VALUES (${values.map((_, i) => '$' + (i + 1)).join(',')})
    ON CONFLICT ("${Object.keys(conflict).join(',')}")
    DO UPDATE SET ${Object.keys(object)
            .map((c) => `"${c}" = EXCLUDED."${c}"`)
            .join(',')}
    RETURNING *`;
        // console.log(query);
        const result = yield postgres.query(query, values);
        return result;
    });
}
exports.upsertObject = upsertObject;
