// =============================================================
// EE-OpenGraph Neo4j Schema
// Estonian public data corruption risk pattern infrastructure
// =============================================================

// ---- CONSTRAINTS (uniqueness) ----

// Person: isikukood is globally unique in Estonia
CREATE CONSTRAINT person_isikukood IF NOT EXISTS
FOR (p:Person) REQUIRE p.isikukood IS UNIQUE;

// Company: registrikood is unique
CREATE CONSTRAINT company_registrikood IF NOT EXISTS
FOR (c:Company) REQUIRE c.registrikood IS UNIQUE;

// NGO: also uses registrikood
CREATE CONSTRAINT ngo_registrikood IF NOT EXISTS
FOR (n:NGO) REQUIRE n.registrikood IS UNIQUE;

// PoliticalParty: by official registry name
CREATE CONSTRAINT party_name IF NOT EXISTS
FOR (pp:PoliticalParty) REQUIRE pp.name IS UNIQUE;

// Procurement: by procedure ID from riigihanked
CREATE CONSTRAINT procurement_id IF NOT EXISTS
FOR (pr:Procurement) REQUIRE pr.procedure_id IS UNIQUE;

// Contract: by contract ID
CREATE CONSTRAINT contract_id IF NOT EXISTS
FOR (ct:Contract) REQUIRE ct.contract_id IS UNIQUE;

// ContractingAuthority: by registry code
CREATE CONSTRAINT authority_code IF NOT EXISTS
FOR (ca:ContractingAuthority) REQUIRE ca.registrikood IS UNIQUE;

// LobbyingMeeting: by composite key (date + official + lobbyist)
CREATE CONSTRAINT lobbying_meeting_id IF NOT EXISTS
FOR (lm:LobbyingMeeting) REQUIRE lm.meeting_id IS UNIQUE;


// ---- INDEXES (search performance) ----

// Person search by name (frequent lookups)
CREATE INDEX person_name IF NOT EXISTS
FOR (p:Person) ON (p.full_name);

// Person search by normalized name (for entity resolution)
CREATE INDEX person_name_normalized IF NOT EXISTS
FOR (p:Person) ON (p.name_normalized);

// Company search by name
CREATE INDEX company_name IF NOT EXISTS
FOR (c:Company) ON (c.name);

// Company by registration date (for timing pattern detection)
CREATE INDEX company_reg_date IF NOT EXISTS
FOR (c:Company) ON (c.registration_date);

// Company by status (active/liquidated/etc)
CREATE INDEX company_status IF NOT EXISTS
FOR (c:Company) ON (c.status);

// Procurement by date (for temporal queries)
CREATE INDEX procurement_date IF NOT EXISTS
FOR (pr:Procurement) ON (pr.published_date);

// Procurement by contracting authority (frequent join)
CREATE INDEX procurement_authority IF NOT EXISTS
FOR (pr:Procurement) ON (pr.authority_registrikood);

// Procurement by estimated value (for threshold analysis)
CREATE INDEX procurement_value IF NOT EXISTS
FOR (pr:Procurement) ON (pr.estimated_value);

// Contract by award date
CREATE INDEX contract_date IF NOT EXISTS
FOR (ct:Contract) ON (ct.award_date);

// Contract by value
CREATE INDEX contract_value IF NOT EXISTS
FOR (ct:Contract) ON (ct.value);


// ---- NODE PROPERTY SCHEMAS (documentation) ----

// Person {
//   isikukood: String (primary key, 11-digit Estonian ID)
//   full_name: String
//   name_normalized: String (lowercase, diacritics removed, for matching)
//   first_name: String
//   last_name: String
//   date_of_birth: Date (derived from isikukood where available)
//   source: String[] (which data sources contributed this node)
//   first_seen: DateTime
//   last_updated: DateTime
// }

// Company {
//   registrikood: String (primary key, 8-digit registry code)
//   name: String
//   status: String (active, liquidated, in_liquidation, struck_off)
//   registration_date: Date
//   legal_form: String (OÜ, AS, TÜ, etc.)
//   address: String
//   kmkr: String (VAT number, if registered)
//   source: String[]
//   first_seen: DateTime
//   last_updated: DateTime
// }

// NGO {
//   registrikood: String (primary key)
//   name: String
//   type: String (MTÜ or SA)
//   registration_date: Date
//   purpose: String
//   source: String[]
//   first_seen: DateTime
//   last_updated: DateTime
// }

// PoliticalParty {
//   name: String (primary key — official registered name)
//   short_name: String (e.g., "RE", "KE", "EKRE", "SDE", "E200")
//   registrikood: String
//   founded_date: Date
//   status: String (active, dissolved)
//   source: String[]
//   last_updated: DateTime
// }

// Procurement {
//   procedure_id: String (primary key — from riigihanked register)
//   title: String
//   description: String
//   cpv_code: String (Common Procurement Vocabulary)
//   procedure_type: String (open, restricted, negotiated, etc.)
//   published_date: Date
//   deadline_date: Date
//   estimated_value: Float
//   currency: String (default EUR)
//   authority_registrikood: String (FK to ContractingAuthority)
//   status: String (published, awarded, cancelled)
//   num_bids: Integer
//   source_url: String
//   source: String[]
//   first_seen: DateTime
//   last_updated: DateTime
// }

// Contract {
//   contract_id: String (primary key)
//   procurement_id: String (FK to Procurement)
//   value: Float
//   currency: String
//   award_date: Date
//   start_date: Date
//   end_date: Date
//   winner_registrikood: String (FK to Company)
//   source: String[]
//   first_seen: DateTime
//   last_updated: DateTime
// }

// ContractingAuthority {
//   registrikood: String (primary key)
//   name: String
//   type: String (ministry, municipality, agency, soe)
//   parent_authority: String (e.g., ministry for sub-agencies)
//   source: String[]
//   last_updated: DateTime
// }

// LobbyingMeeting {
//   meeting_id: String (primary key — generated composite)
//   date: Date
//   official_name: String
//   official_institution: String
//   lobbyist_name: String
//   lobbyist_organization: String
//   topic: String
//   source_url: String
//   source: String[]
//   first_seen: DateTime
// }
