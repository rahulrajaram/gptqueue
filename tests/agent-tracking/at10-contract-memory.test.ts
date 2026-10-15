/** AT10: the seat contract suite against the in-memory store. */
import { createMemorySeatStore } from "../../src/agent-tracking/index.js";
import { seatContract } from "./seat-contract.js";

seatContract("memory store", () => createMemorySeatStore());
