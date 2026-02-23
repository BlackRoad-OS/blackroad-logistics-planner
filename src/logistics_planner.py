"""Logistics and route planning system."""

import sqlite3
import json
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from pathlib import Path
import argparse
import uuid
import math

# Database initialization
DB_PATH = Path.home() / ".blackroad" / "logistics.db"

SHIPMENT_STATUSES = ["pending", "picked_up", "in_transit", "out_for_delivery", "delivered", "exception"]
PRIORITIES = ["standard", "express", "overnight"]
CARRIERS = ["fedex", "ups", "usps", "dhl", "blackroad-express"]

# Major city coordinates (latitude, longitude)
CITY_COORDS = {
    "NYC": (40.7128, -74.0060),
    "LAX": (34.0522, -118.2437),
    "CHI": (41.8781, -87.6298),
    "HOU": (29.7604, -95.3698),
    "PHX": (33.4484, -112.0742),
    "PHI": (39.9526, -75.1652),
    "SAN": (32.7157, -117.1611),
    "DAL": (32.7767, -96.7970),
    "SJC": (37.3382, -121.8863),
    "AUS": (30.2672, -97.7431),
    "DEN": (39.7392, -104.9903),
    "ATL": (33.7490, -84.3880),
    "BOS": (42.3601, -71.0589),
    "MIA": (25.7617, -80.1918),
    "SEA": (47.6062, -122.3321),
    "POR": (45.5152, -122.6784),
    "MIN": (44.9778, -93.2650),
    "DET": (42.3314, -83.0458),
    "CLE": (41.4993, -81.6944),
    "STL": (38.6270, -90.1994),
}


@dataclass
class Shipment:
    """Represents a shipment."""
    id: str
    origin: str
    destination: str
    weight_kg: float
    priority: str
    status: str
    eta: Optional[str] = None
    carrier: Optional[str] = None
    tracking_id: Optional[str] = None
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return asdict(self)


class LogisticsPlanner:
    """Logistics and route planning system."""

    def __init__(self, db_path: Path = DB_PATH):
        """Initialize the planner with SQLite database."""
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        """Initialize SQLite database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS shipments (
                    id TEXT PRIMARY KEY,
                    origin TEXT NOT NULL,
                    destination TEXT NOT NULL,
                    weight_kg REAL NOT NULL,
                    priority TEXT NOT NULL,
                    status TEXT NOT NULL,
                    eta TEXT,
                    carrier TEXT,
                    tracking_id TEXT,
                    created_at TEXT NOT NULL
                )
            """)
            conn.commit()

    @staticmethod
    def _haversine_distance(coord1: tuple, coord2: tuple) -> float:
        """Calculate distance between two coordinates using Haversine formula."""
        lat1, lon1 = coord1
        lat2, lon2 = coord2
        
        R = 6371  # Earth radius in km
        
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lon = math.radians(lon2 - lon1)
        
        a = math.sin(delta_lat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
        c = 2 * math.asin(math.sqrt(a))
        
        return R * c

    def create_shipment(
        self,
        origin: str,
        destination: str,
        weight_kg: float,
        priority: str = "standard",
    ) -> str:
        """Create a new shipment."""
        if priority not in PRIORITIES:
            raise ValueError(f"Invalid priority. Must be one of: {PRIORITIES}")

        shipment_id = str(uuid.uuid4())[:8]
        shipment = Shipment(
            id=shipment_id,
            origin=origin,
            destination=destination,
            weight_kg=weight_kg,
            priority=priority,
            status="pending",
        )

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO shipments VALUES (
                    :id, :origin, :destination, :weight_kg, :priority, :status,
                    :eta, :carrier, :tracking_id, :created_at
                )
                """,
                shipment.to_dict(),
            )
            conn.commit()

        return shipment_id

    def assign_carrier(
        self,
        shipment_id: str,
        carrier: str,
        tracking_id: str,
        eta_days: int,
    ):
        """Assign a carrier to a shipment."""
        if carrier not in CARRIERS:
            raise ValueError(f"Invalid carrier. Must be one of: {CARRIERS}")

        eta = (datetime.utcnow() + timedelta(days=eta_days)).isoformat()

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                UPDATE shipments SET carrier = ?, tracking_id = ?, eta = ?, status = ?
                WHERE id = ?
                """,
                (carrier, tracking_id, eta, "picked_up", shipment_id),
            )
            conn.commit()

    def update_status(self, shipment_id: str, status: str):
        """Update shipment status."""
        if status not in SHIPMENT_STATUSES:
            raise ValueError(f"Invalid status. Must be one of: {SHIPMENT_STATUSES}")

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE shipments SET status = ? WHERE id = ?",
                (status, shipment_id),
            )
            conn.commit()

    def get_shipments(
        self,
        status: Optional[str] = None,
        priority: Optional[str] = None,
    ) -> List[Shipment]:
        """Get shipments, optionally filtered by status and/or priority."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            query = "SELECT * FROM shipments WHERE 1=1"
            params = []
            
            if status:
                query += " AND status = ?"
                params.append(status)
            if priority:
                query += " AND priority = ?"
                params.append(priority)
            
            query += " ORDER BY created_at DESC"
            cursor = conn.execute(query, params)
            rows = cursor.fetchall()

        return [Shipment(**dict(row)) for row in rows]

    def get_route(self, origin: str, destination: str) -> Dict[str, Any]:
        """Get route information between two cities."""
        if origin not in CITY_COORDS or destination not in CITY_COORDS:
            return {
                "error": f"Unknown city. Available cities: {list(CITY_COORDS.keys())}"
            }

        distance_km = self._haversine_distance(
            CITY_COORDS[origin],
            CITY_COORDS[destination],
        )
        
        # Rough estimate: average truck speed 80 km/h
        duration_h = distance_km / 80

        return {
            "origin": origin,
            "destination": destination,
            "distance_km": round(distance_km, 1),
            "duration_h": round(duration_h, 1),
            "stops": [origin, destination],
        }

    def optimize_batch(self, shipment_ids: List[str]) -> Dict[str, Any]:
        """Optimize batch of shipments by grouping by carrier and region."""
        shipments = []
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            for sid in shipment_ids:
                cursor = conn.execute(
                    "SELECT * FROM shipments WHERE id = ?", (sid,)
                )
                row = cursor.fetchone()
                if row:
                    shipments.append(Shipment(**dict(row)))

        # Group by carrier
        by_carrier = {}
        for s in shipments:
            if s.carrier:
                if s.carrier not in by_carrier:
                    by_carrier[s.carrier] = []
                by_carrier[s.carrier].append(s)

        # Group by priority
        by_priority = {}
        for s in shipments:
            if s.priority not in by_priority:
                by_priority[s.priority] = []
            by_priority[s.priority].append(s)

        return {
            "total_shipments": len(shipments),
            "by_carrier": {
                carrier: len(ships) for carrier, ships in by_carrier.items()
            },
            "by_priority": {
                priority: len(ships) for priority, ships in by_priority.items()
            },
        }

    def delivery_stats(self) -> Dict[str, Any]:
        """Calculate delivery performance statistics."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT * FROM shipments")
            rows = cursor.fetchall()

        shipments = [Shipment(**dict(row)) for row in rows]

        delivered = [s for s in shipments if s.status == "delivered"]
        exception = [s for s in shipments if s.status == "exception"]

        # Calculate on-time delivery rate
        on_time_count = 0
        if delivered:
            for s in delivered:
                if s.eta:
                    eta_time = datetime.fromisoformat(s.eta)
                    if eta_time >= datetime.utcnow():
                        on_time_count += 1

        on_time_rate = (on_time_count / len(delivered) * 100) if delivered else 0

        # Calculate average transit time
        avg_transit_days = 0
        if delivered:
            transit_times = []
            for s in delivered:
                created = datetime.fromisoformat(s.created_at)
                # Use eta as proxy for completion time
                if s.eta:
                    eta_time = datetime.fromisoformat(s.eta)
                    transit_days = (eta_time - created).days
                    transit_times.append(transit_days)
            if transit_times:
                avg_transit_days = sum(transit_times) / len(transit_times)

        # Performance by carrier
        by_carrier = {}
        for s in shipments:
            if s.carrier:
                if s.carrier not in by_carrier:
                    by_carrier[s.carrier] = {"delivered": 0, "total": 0}
                by_carrier[s.carrier]["total"] += 1
                if s.status == "delivered":
                    by_carrier[s.carrier]["delivered"] += 1

        carrier_performance = {
            carrier: {
                "delivery_rate": round(
                    (stats["delivered"] / stats["total"] * 100), 1
                )
            }
            for carrier, stats in by_carrier.items()
        }

        return {
            "total_shipments": len(shipments),
            "delivered": len(delivered),
            "in_exception": len(exception),
            "on_time_rate_pct": round(on_time_rate, 1),
            "avg_transit_days": round(avg_transit_days, 1),
            "by_carrier": carrier_performance,
        }


def main():
    """CLI interface for logistics planner."""
    parser = argparse.ArgumentParser(description="Logistics Planner")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # List command
    list_parser = subparsers.add_parser("list", help="List shipments")
    list_parser.add_argument("--status", help="Filter by status")
    list_parser.add_argument("--priority", help="Filter by priority")

    # Create command
    create_parser = subparsers.add_parser("create", help="Create a new shipment")
    create_parser.add_argument("origin", help="Origin city")
    create_parser.add_argument("destination", help="Destination city")
    create_parser.add_argument("weight_kg", type=float, help="Weight in kg")
    create_parser.add_argument(
        "priority",
        nargs="?",
        default="standard",
        help="Priority (standard/express/overnight)",
    )

    # Stats command
    stats_parser = subparsers.add_parser("stats", help="Delivery statistics")

    # Route command
    route_parser = subparsers.add_parser("route", help="Get route info")
    route_parser.add_argument("origin", help="Origin city")
    route_parser.add_argument("destination", help="Destination city")

    args = parser.parse_args()
    planner = LogisticsPlanner()

    if args.command == "list":
        shipments = planner.get_shipments(status=args.status, priority=args.priority)
        if not shipments:
            print("No shipments found.")
            return
        print(f"{'ID':<10} {'Origin':<8} {'Dest':<8} {'Status':<18} {'Priority':<10}")
        print("-" * 60)
        for s in shipments:
            print(f"{s.id:<10} {s.origin:<8} {s.destination:<8} {s.status:<18} {s.priority:<10}")

    elif args.command == "create":
        sid = planner.create_shipment(
            args.origin,
            args.destination,
            args.weight_kg,
            priority=args.priority,
        )
        print(f"Created shipment {sid}: {args.origin} → {args.destination}")

    elif args.command == "stats":
        stats = planner.delivery_stats()
        print(json.dumps(stats, indent=2))

    elif args.command == "route":
        route = planner.get_route(args.origin, args.destination)
        print(json.dumps(route, indent=2))

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
