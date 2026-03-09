"""
AURUM-HFT :: Module 01 :: data_bridge.py
=========================================
Ingestion tick-by-tick asynchrone via AllTick API.
- Rate limiting respectueux du tier gratuit (1 req / 10s)
- Reconnexion automatique avec backoff exponentiel
- Normalisation des ticks vers un format interne unifié
- Publication sur le bus d'événements interne (asyncio.Queue)

Dépendances :
    pip install aiohttp python-dotenv pydantic

Configuration (.env) :
    ALLTICK_API_KEY=votre_cle_ici
    ALLTICK_SYMBOL=XAUUSD
    TICK_QUEUE_SIZE=10000
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import AsyncIterator, Callable, Optional

import aiohttp
from dotenv import load_dotenv
from pydantic import BaseModel, Field, validator

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
load_dotenv()

log = logging.getLogger("aurum.data_bridge")

# AllTick REST endpoint (tier gratuit — pas de WebSocket)
ALLTICK_BASE_URL = "https://quote.alltick.io/quote-stock-b-api/trade"

# Délai minimum entre deux requêtes (tier gratuit = 10s)
FREE_TIER_INTERVAL_S: float = 10.0

# Retry policy
MAX_RETRIES: int = 5
RETRY_BASE_DELAY_S: float = 2.0   # sera doublé à chaque tentative


# ---------------------------------------------------------------------------
# Modèles de données
# ---------------------------------------------------------------------------

class RawTickAllTick(BaseModel):
    """Réponse brute d'AllTick — structure minimale garantie."""
    code: str = Field(..., description="Symbole, ex: XAUUSD")
    timestamp: int = Field(..., description="Unix ms")
    last_price: float = Field(..., alias="last_price")
    bid: Optional[float] = Field(None)
    ask: Optional[float] = Field(None)
    volume: Optional[float] = Field(None)

    @validator("last_price", "bid", "ask", pre=True)
    def coerce_float(cls, v):
        if v is None:
            return None
        return float(v)

    class Config:
        populate_by_name = True


@dataclass(slots=True, frozen=True)
class NormalizedTick:
    """
    Format interne unifié. Tous les modules consomment ce type.
    Immutable par design (frozen dataclass).
    """
    symbol: str
    ts: datetime          # UTC aware
    ts_ms: int            # epoch milliseconds
    price: float          # last traded price
    bid: float
    ask: float
    spread: float         # ask - bid en pips
    volume: float
    mid: float            # (bid + ask) / 2
    source: str = "alltick"

    @classmethod
    def from_raw(cls, raw: RawTickAllTick) -> "NormalizedTick":
        bid = raw.bid or raw.last_price
        ask = raw.ask or raw.last_price
        return cls(
            symbol=raw.code,
            ts=datetime.fromtimestamp(raw.timestamp / 1000, tz=timezone.utc),
            ts_ms=raw.timestamp,
            price=raw.last_price,
            bid=bid,
            ask=ask,
            spread=round(ask - bid, 5),
            volume=raw.volume or 0.0,
            mid=round((bid + ask) / 2, 5),
        )

    def __str__(self) -> str:
        return (
            f"[{self.ts.isoformat()}] {self.symbol} "
            f"price={self.price:.3f} bid={self.bid:.3f} "
            f"ask={self.ask:.3f} spread={self.spread:.5f}"
        )


# ---------------------------------------------------------------------------
# Rate Limiter asynchrone
# ---------------------------------------------------------------------------

class AsyncRateLimiter:
    """
    Token-bucket simple : garantit un délai minimum entre les appels.
    Thread-safe via asyncio.Lock.
    """

    def __init__(self, min_interval_s: float):
        self._min_interval = min_interval_s
        self._last_call: float = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_call
            wait = self._min_interval - elapsed
            if wait > 0:
                log.debug("RateLimiter: attente %.2fs", wait)
                await asyncio.sleep(wait)
            self._last_call = time.monotonic()


# ---------------------------------------------------------------------------
# DataBridge principal
# ---------------------------------------------------------------------------

class DataBridge:
    """
    Orchestre la connexion à AllTick et produit un flux de NormalizedTick
    via un asyncio.Queue consommable par les modules aval.

    Usage:
        bridge = DataBridge()
        await bridge.start()

        async for tick in bridge.stream():
            print(tick)
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        symbol: Optional[str] = None,
        queue_size: int = 10_000,
        interval_s: float = FREE_TIER_INTERVAL_S,
        on_tick: Optional[Callable[[NormalizedTick], None]] = None,
    ):
        self._api_key = api_key or os.environ.get("ALLTICK_API_KEY")
        if not self._api_key:
            raise EnvironmentError(
                "ALLTICK_API_KEY manquante. "
                "Définissez-la dans .env ou passez api_key= au constructeur."
            )

        self._symbol = (symbol or os.environ.get("ALLTICK_SYMBOL", "XAUUSD")).upper()
        self._queue: asyncio.Queue[NormalizedTick] = asyncio.Queue(maxsize=queue_size)
        self._rate_limiter = AsyncRateLimiter(interval_s)
        self._on_tick = on_tick   # callback optionnel synchrone

        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._session: Optional[aiohttp.ClientSession] = None

        # Métriques légères
        self._total_ticks: int = 0
        self._total_errors: int = 0
        self._last_tick_ts: Optional[float] = None

    # ------------------------------------------------------------------
    # Cycle de vie
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Démarre la boucle d'ingestion en arrière-plan."""
        if self._running:
            log.warning("DataBridge déjà démarré.")
            return
        self._session = aiohttp.ClientSession(
            headers={
                "Content-Type": "application/json",
                "token": self._api_key,
            },
            timeout=aiohttp.ClientTimeout(total=15),
        )
        self._running = True
        self._task = asyncio.create_task(self._poll_loop(), name="data_bridge_poll")
        log.info("DataBridge démarré — symbole=%s intervalle=%.1fs", self._symbol, FREE_TIER_INTERVAL_S)

    async def stop(self) -> None:
        """Arrêt propre."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._session and not self._session.closed:
            await self._session.close()
        log.info(
            "DataBridge arrêté — ticks_reçus=%d erreurs=%d",
            self._total_ticks, self._total_errors,
        )

    # ------------------------------------------------------------------
    # Boucle de polling
    # ------------------------------------------------------------------

    async def _poll_loop(self) -> None:
        """
        Boucle principale : fetch → parse → normalise → enqueue.
        Gère les erreurs réseau avec backoff exponentiel.
        """
        retry_count = 0

        while self._running:
            await self._rate_limiter.acquire()

            try:
                raw = await self._fetch_tick()
                tick = NormalizedTick.from_raw(raw)

                # Enqueue (drop si pleine — évite le blocage du producteur)
                try:
                    self._queue.put_nowait(tick)
                except asyncio.QueueFull:
                    log.warning("Queue pleine — tick ignoré: %s", tick)

                if self._on_tick:
                    self._on_tick(tick)

                self._total_ticks += 1
                self._last_tick_ts = time.monotonic()
                retry_count = 0  # reset après succès
                log.debug("Tick reçu: %s", tick)

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                self._total_errors += 1
                retry_count += 1
                delay = min(RETRY_BASE_DELAY_S * (2 ** retry_count), 120.0)
                log.error(
                    "Erreur réseau (tentative %d/%d): %s — retry dans %.1fs",
                    retry_count, MAX_RETRIES, exc, delay,
                )
                if retry_count >= MAX_RETRIES:
                    log.critical("MAX_RETRIES atteint. DataBridge suspendu.")
                    self._running = False
                    break
                await asyncio.sleep(delay)

            except Exception as exc:  # noqa: BLE001
                self._total_errors += 1
                log.exception("Erreur inattendue dans _poll_loop: %s", exc)

    # ------------------------------------------------------------------
    # Appel HTTP AllTick
    # ------------------------------------------------------------------

    async def _fetch_tick(self) -> RawTickAllTick:
        """
        Appelle l'endpoint AllTick /trade et retourne un RawTickAllTick.

        Endpoint : GET /quote-stock-b-api/trade?code=XAUUSD&num=1
        """
        params = {"code": self._symbol, "num": "1"}

        assert self._session is not None  # start() doit avoir été appelé
        async with self._session.get(ALLTICK_BASE_URL, params=params) as resp:
            resp.raise_for_status()
            payload = await resp.json()

        # AllTick enveloppe la réponse dans {"data": {"tick": [...]}}
        tick_list = payload.get("data", {}).get("tick", [])
        if not tick_list:
            raise ValueError(f"Réponse AllTick vide ou inattendue: {payload}")

        t = tick_list[0]
        return RawTickAllTick(
            code=t.get("code", self._symbol),
            timestamp=int(t.get("time_stamp", int(time.time() * 1000))),
            last_price=t["price"],
            bid=t.get("bid"),
            ask=t.get("ask"),
            volume=t.get("volume"),
        )

    # ------------------------------------------------------------------
    # Interface consommateur
    # ------------------------------------------------------------------

    async def stream(self) -> AsyncIterator[NormalizedTick]:
        """
        Itérateur asynchrone sur les ticks normalisés.

        Exemple:
            async for tick in bridge.stream():
                process(tick)
        """
        while self._running or not self._queue.empty():
            try:
                tick = await asyncio.wait_for(self._queue.get(), timeout=30.0)
                yield tick
                self._queue.task_done()
            except asyncio.TimeoutError:
                if not self._running:
                    break

    async def get_tick(self, timeout: float = 30.0) -> NormalizedTick:
        """Récupère un seul tick (usage ponctuel)."""
        return await asyncio.wait_for(self._queue.get(), timeout=timeout)

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    @property
    def stats(self) -> dict:
        lag = None
        if self._last_tick_ts is not None:
            lag = round(time.monotonic() - self._last_tick_ts, 2)
        return {
            "symbol": self._symbol,
            "running": self._running,
            "total_ticks": self._total_ticks,
            "total_errors": self._total_errors,
            "queue_size": self._queue.qsize(),
            "last_tick_lag_s": lag,
        }


# ---------------------------------------------------------------------------
# Point d'entrée de test (python data_bridge.py)
# ---------------------------------------------------------------------------

async def _demo() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    bridge = DataBridge()
    await bridge.start()

    print("=== AURUM-HFT DataBridge — flux live XAUUSD ===")
    print("Ctrl+C pour arrêter\n")

    try:
        async for tick in bridge.stream():
            print(tick)
            print("Stats:", bridge.stats)
    except KeyboardInterrupt:
        pass
    finally:
        await bridge.stop()


if __name__ == "__main__":
    asyncio.run(_demo())
