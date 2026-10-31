export type Json = any;
export type Mode = "Lexical" | "Vector" | "Hybrid";
export type SchemaField = {
  field: string;
  field_type: string;
  store?: boolean;
  index_lexical?: boolean;
  index_vector?: boolean;
  filterable?: boolean;
  boost?: number;
};
export type Collection = {
  id: string;
  name: string;
  schema: SchemaField[];
  indexed_doc_count: number;
  version: string;
  embedding_space_id?: string;
  vector_dimensions?: number | null;
  statistics: Json;
  limits: Json;
};
export type RecordData = {
  record_id: string;
  version: number;
  document: Record<string, Json> | null;
  [key: string]: Json;
};
export type Hit = { record: RecordData; score?: number; channels?: Json[] };
export type Page = {
  records: RecordData[];
  next: Json;
  visible_seq: number;
  evaluation_time_ms: number;
};
export type QueryResult = {
  hits: Hit[];
  count: number;
  count_total: number | null;
  time: number;
  evaluation_time_ms: number;
  [key: string]: Json;
};
