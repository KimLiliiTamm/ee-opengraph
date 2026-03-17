# EE-OpenGraph (ee/acc)

## What this project is
Open-source graph infrastructure that cross-references Estonia's public databases
(business registry, procurement, political financing, beneficial ownership, lobbying)
to surface corruption risk patterns. Inspired by br/acc (Brazil).
No accusations — surfaces connections and lets users draw conclusions.

## Architecture
- Graph DB: Neo4j 5 Community (with APOC plugin)
- Backend: FastAPI (Python 3.12+, async)
- ETL: Python (httpx, pandas, lxml for XML parsing)
- Frontend: Vite + React 19 + TypeScript (later phases)
- Infra: Docker Compose
- Entity resolution: isikukood-first, fuzzy fallback

## Data sources

### Phase 1 (current)
1. **Äriregister** (RIK API) — companies, board members, shareholders, beneficial owners
   - API: https://ariregister.rik.ee (JSON, XML, CSV bulk)
   - Key: registrikood for companies, isikukood for persons
   - Free access including UBO data

2. **ERJK** — political party donations, quarterly reports, enforcement actions
   - Data: https://www.erjk.ee (downloadable structured data)
   - Donation data includes: donor name, isikukood (partial), amount, date, party

3. **Riigihangete Register** — public procurement
   - API: https://riigihanked.riik.ee (XML REST API)
   - NOT in OCDS format — custom XML, needs parser
   - ~200,000+ historical procedures

4. **Lobbying register** — meeting logs
   - Source: korruptsioon.ee (mandatory since March 2021)
   - ~700+ meetings published in first year

### Phase 2 (planned)
- Asset declarations (electronic register, requires digital ID)
- Court decisions (politsei.ee, Riigi Teataja)
- EU structural funds (struktuurifondid.ee)

## Neo4j schema

### Node labels (PascalCase)
- `Person` — natural person (isikukood as primary key)
- `Company` — legal entity (registrikood as primary key)
- `PoliticalParty` — registered political party
- `NGO` — MTÜ/SA (foundation/nonprofit, registrikood as key)
- `Procurement` — public procurement procedure
- `Contract` — awarded contract within a procurement
- `ContractingAuthority` — ministry, municipality, state agency
- `LobbyingMeeting` — logged meeting between lobbyist and official

### Relationship types (UPPER_SNAKE_CASE)
All relationships with temporal properties where applicable (start_date, end_date).

- `BOARD_MEMBER_OF` {start_date, end_date, role}
- `SHAREHOLDER_OF` {start_date, end_date, percentage}
- `BENEFICIAL_OWNER_OF` {start_date, end_date}
- `DONATED_TO` {amount, date, quarter}
- `MEMBER_OF` {start_date, end_date} (party membership)
- `AWARDED_TO` {value, date}
- `SUBMITTED_BID` {date, value}
- `ISSUED_BY` {date}
- `SUBCONTRACTED_TO` {value, contract_id}
- `MET_WITH` {date, topic, institution}
- `CONNECTED_PERSON_OF` {relationship_type} (KVS §7)
- `CHANNELED_FUNDS_TO` {amount, period} (NGO→Party)

## Entity resolution rules
1. **Deterministic**: Exact isikukood match → same person (always)
2. **High confidence**: Full name + date of birth match → same person
3. **Medium confidence**: Normalized name match + shared company connection → likely same person
4. **Low confidence / ambiguous**: Flag for manual review or LLM disambiguation
- Common Estonian names (Jaan Tamm, Andres Kask) require extra context
- ERJK partial isikukood + name should match against full isikukood from Äriregister

## ETL conventions
- Each pipeline is a standalone Python module in `etl/<source>/`
- Common utilities in `etl/common/` (http client, retry logic, neo4j writer, entity resolution)
- All pipelines write to Neo4j via the bolt driver (neo4j Python package)
- Config via environment variables (see .env.example)
- Logging: structured with source attribution
- Error handling: continue on individual record failures, log and summarize
- Rate limiting: respect source API limits, use exponential backoff

## Pattern detection (analysis/)
Automated corruption risk patterns implemented as Cypher queries:
1. Registration-to-award timing (company created <90 days before first contract win)
2. Board rotation (person moves between companies that bid on same tenders)
3. Bid clustering (companies always appear together, never compete elsewhere)
4. Donation-contract correlation (donor's companies win contracts from donor's party's ministries)
5. Value clustering below thresholds (anomalous concentration just below €30,000)
6. Influence peddling chains (Porto Franco pattern: donor→party→pressure→favorable outcome)
7. NGO money channeling (Isamaalized pattern: donor→NGO→party services)
8. Revolving door with lobbying (former official lobbies former ministry, company wins contract)

## Legal basis
All data is public under Estonian law:
- Avaliku teabe seadus (Public Information Act)
- Äriseadustik §28 (Commercial Code — registry publicity)
- Riigihangete seadus (Public Procurement Act — transparency)
- Erakondade rahastamise ja erakondadele kehtestatud piirangute järelevalve seadus
- GDPR Art. 6(1)(e) — processing in public interest
- Korruptsioonivastane seadus (Anti-Corruption Act) — public declarations

## Code style
- Python: black formatter, ruff linter, type hints everywhere
- Tests: pytest
- Commits: conventional commits (feat:, fix:, docs:, refactor:, test:)
- Docstrings: Google style
- All Cypher queries in .cypher files, not inline strings

## What NOT to do
- Never store personal data beyond what's publicly available in registries
- Never make accusations — surface patterns and connections only
- Never scrape authenticated endpoints without explicit legal basis
- Never hardcode credentials (use .env)
- Never commit data files to git (use .gitignore)
- Never ignore rate limits on public APIs
