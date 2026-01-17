import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  vus: 2,
  duration: '30s',
};

const BASE_URL = 'http://localhost:8000';
const TABLE_NAME = 'test_table';

export function setup() {
  const params = {
    headers: { 'Content-Type': 'application/json' },
  };

  const createTablePayload = JSON.stringify({
    name: TABLE_NAME,
    columns: [
      { name: 'id', type: 0, is_indexed: true },
      { name: 'name', type: 1, length: 100, is_indexed: false },
      { name: 'age', type: 0, is_indexed: true },
      { name: 'birth_date', type: 2, is_indexed: false },
      { name: 'created_at', type: 3, is_indexed: false },
      { name: 'score', type: 4, is_indexed: false },
      { name: 'metadata', type: 5, is_indexed: false },
    ],
  });

  const res = http.post(
    `${BASE_URL}/api/v1/tables/create-table`,
    createTablePayload,
    params
  );

  console.log('Table created:', res.status);
}

export default function () {
  const params = {
    headers: { 'Content-Type': 'application/json' },
  };

  const id = Math.floor(Math.random() * 1000000);

  const insertPayload = JSON.stringify({
    name: TABLE_NAME,
    values: {
      id: id,
      name: `User_${id}`,
      age: Math.floor(Math.random() * 80) + 18,
      birth_date: '2000-01-15',
      created_at: '2026-01-15T17:15:00Z',
      score: Math.random() * 100,
      metadata: '{"key":"value"}',
    },
  });

  http.post(`${BASE_URL}/api/v1/records/insert`, insertPayload, params);

  const queryPayload = JSON.stringify({
    name: TABLE_NAME,
    filter: [],
  });

  http.post(`${BASE_URL}/api/v1/records/query`, queryPayload, params);

  sleep(1);
}
