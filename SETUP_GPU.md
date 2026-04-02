# Configuració PC amb GPU (Windows + CUDA)

Instruccions per executar el pipeline complet de lobbyscope.cat
en un PC Windows amb GPU NVIDIA i CUDA instal·lat.

## Prerequisits

- Windows 10/11
- NVIDIA GPU amb CUDA 11.8+ instal·lat
- Python 3.10–3.12 (recomanat 3.11; **evitar Windows Store Python**)
- Docker Desktop (per a PostgreSQL + pgvector)
- Git

## 1. Clonar i configurar entorn

```powershell
git clone https://github.com/<usuari>/lobbies.git
cd lobbies

python -m venv venv
venv\Scripts\activate
```

## 2. Instal·lar PyTorch amb suport CUDA

**Important**: instal·lar PyTorch PRIMER, abans de `requirements.txt`.

```powershell
# CUDA 12.1 (comprova la teva versió amb `nvidia-smi`)
pip install torch --index-url https://download.pytorch.org/whl/cu121

# O per CUDA 11.8:
# pip install torch --index-url https://download.pytorch.org/whl/cu118
```

Verificar que detecta la GPU:

```powershell
python -c "import torch; print(torch.cuda.is_available(), torch.cuda.get_device_name(0))"
```

## 3. Instal·lar la resta de dependències

```powershell
pip install -r requirements.txt
```

## 4. Configurar variables d'entorn

```powershell
copy .env.example .env
# Editar .env amb les credencials reals
```

Contingut mínim del `.env`:

```
DATABASE_URL=postgresql://postgres:password@localhost:5432/lobbyscope
PGVECTOR_ENABLED=true
ANTHROPIC_API_KEY=sk-ant-...
SOCRATA_APP_TOKEN=          # opcional
EMBEDDING_MODEL=sentence-transformers/LaBSE
```

## 5. Arrancar PostgreSQL amb pgvector

```powershell
docker compose up -d
```

Verificar que pgvector funciona:

```powershell
docker compose exec db psql -U postgres -d lobbyscope -c "SELECT vector_dims('[1,2,3]'::vector);"
```

## 6. Executar el pipeline d'ingesta

```powershell
# Ordre recomanat per a primera ingesta completa:
python run_ingesta.py --fase agendes
python run_ingesta.py --fase grups
python run_ingesta.py --fase dogc
python run_ingesta.py --fase acords_govern
python run_ingesta.py --fase subvencions
python run_ingesta.py --fase contractes

# Crear índexs vectorials ivfflat (DESPRÉS de tenir dades)
docker compose exec db psql -U postgres -d lobbyscope -f /docker-entrypoint-initdb.d/create_vector_indexes.sql
# O directament:
# psql $DATABASE_URL -f db/create_vector_indexes.sql

# Generar embeddings (usa GPU automàticament si torch.cuda.is_available())
python run_ingesta.py --fase embeddings

# Detectar connexions i calcular scores
python run_ingesta.py --fase connexions
python run_ingesta.py --fase scores

# Verificar recomptes finals
python run_ingesta.py --fase stats
```

## 7. Acceleració GPU per a embeddings

El mòdul `processament/embeddings.py` usa `sentence-transformers` que
detecta automàticament CUDA. No cal cap canvi de codi.

Per verificar que s'usa la GPU durant els embeddings:

```powershell
# En una altra terminal mentre corren els embeddings:
nvidia-smi
```

Amb GPU, el rendiment esperat:
- **CPU**: ~128 embeddings/min
- **GPU (RTX 3080+)**: ~3.000–8.000 embeddings/min

## 8. Temps estimats amb GPU

| Dataset         | Registres | GPU (est.)  |
|-----------------|-----------|-------------|
| reunions        | ~47.000   | ~10 min     |
| normativa_dogc  | ~4.330    | ~1 min      |
| acords_govern   | ~12.631   | ~3 min      |
| subvencions     | ~28.000   | ~6 min      |
| contractes      | ~177.000  | ~35 min     |

## Notes

- El model LaBSE (~1.8 GB) es descarrega automàticament en el primer ús
  a `~/.cache/huggingface/hub/`. Amb connexió lenta, pot trigar.
- Si el PC no té GPU, els embeddings funcionen en CPU (molt lent per a contractes).
- `PGVECTOR_ENABLED=true` és necessari per usar les columnes `vector(768)` reals
  i les cerques ANN amb ivfflat.
