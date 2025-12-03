import sqlite3
import random
import string
from datetime import datetime
from typing import Optional, List, Tuple
import os


class DistributionCenterDB:
    """Database manager for the distribution center package management system."""
    
    def __init__(self, db_name: str = "distribution_center.db"):
        self.db_name = db_name
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Establish database connection."""
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        self.conn.execute("PRAGMA foreign_keys = ON")
        
    def disconnect(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            
    def initialize_database(self):
        """Create database schema and populate initial data."""
        
        # Create Categories table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS Categories (
                category_id INTEGER PRIMARY KEY AUTOINCREMENT,
                category_name VARCHAR(50) UNIQUE NOT NULL,
                description TEXT,
                zone VARCHAR(10) NOT NULL,
                max_weight REAL,
                priority_level INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create Locations table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS Locations (
                location_id INTEGER PRIMARY KEY AUTOINCREMENT,
                location_code VARCHAR(20) UNIQUE NOT NULL,
                zone VARCHAR(10) NOT NULL,
                aisle INTEGER NOT NULL,
                shelf INTEGER NOT NULL,
                category_id INTEGER,
                is_occupied BOOLEAN DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (category_id) REFERENCES Categories(category_id)
            )
        """)
        
        # Create Packages table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS Packages (
                package_id INTEGER PRIMARY KEY AUTOINCREMENT,
                barcode VARCHAR(50) UNIQUE NOT NULL,
                weight REAL NOT NULL,
                length REAL NOT NULL,
                width REAL NOT NULL,
                height REAL NOT NULL,
                destination VARCHAR(100) NOT NULL,
                priority VARCHAR(20) NOT NULL,
                category_id INTEGER NOT NULL,
                location_id INTEGER,
                status VARCHAR(20) DEFAULT 'Received',
                received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (category_id) REFERENCES Categories(category_id),
                FOREIGN KEY (location_id) REFERENCES Locations(location_id)
            )
        """)
        
        # Create Audit Trail table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS AuditTrail (
                audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                package_id INTEGER NOT NULL,
                action VARCHAR(50) NOT NULL,
                old_status VARCHAR(20),
                new_status VARCHAR(20),
                old_location VARCHAR(20),
                new_location VARCHAR(20),
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                notes TEXT,
                FOREIGN KEY (package_id) REFERENCES Packages(package_id)
            )
        """)
        
        # Create indexes for better performance
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_packages_barcode 
            ON Packages(barcode)
        """)
        
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_packages_category 
            ON Packages(category_id)
        """)
        
        self.cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_locations_zone 
            ON Locations(zone)
        """)
        
        self.conn.commit()
        
        # Populate initial data
        self._populate_initial_data()
        
    def _populate_initial_data(self):
        """Populate categories and locations."""
        
        # Insert categories
        categories = [
            ('Standard', 'Regular packages, standard delivery', 'A', 30.0, 3),
            ('Express', 'High priority, expedited delivery', 'B', 25.0, 1),
            ('Fragile', 'Handle with care, delicate items', 'C', 20.0, 2),
            ('Heavy', 'Heavy items requiring special handling', 'D', 100.0, 4),
            ('International', 'International shipments', 'E', 50.0, 2)
        ]
        
        try:
            self.cursor.executemany("""
                INSERT OR IGNORE INTO Categories 
                (category_name, description, zone, max_weight, priority_level)
                VALUES (?, ?, ?, ?, ?)
            """, categories)
            
            # Create locations for each category
            for cat_id in range(1, 6):
                for aisle in range(1, 6):
                    for shelf in range(1, 5):
                        zone = chr(64 + cat_id)  # A, B, C, D, E
                        location_code = f"{zone}{aisle:02d}-{shelf:02d}"
                        
                        self.cursor.execute("""
                            INSERT OR IGNORE INTO Locations 
                            (location_code, zone, aisle, shelf, category_id)
                            VALUES (?, ?, ?, ?, ?)
                        """, (location_code, zone, aisle, shelf, cat_id))
            
            self.conn.commit()
        except sqlite3.IntegrityError:
            pass  # Data already exists


class PackageManager:
    """Manager for package operations."""
    
    def __init__(self, db: DistributionCenterDB):
        self.db = db
        
    def categorize_package(self, weight: float, priority: str, 
                          destination: str) -> Tuple[int, str]:
        """
        Determine package category based on attributes.
        
        Returns: (category_id, category_name)
        """
        # Express priority
        if priority.lower() == 'express':
            return (2, 'Express')
        
        # International destination
        if 'international' in destination.lower() or destination.count(',') > 1:
            return (5, 'International')
        
        # Heavy items
        if weight > 50.0:
            return (4, 'Heavy')
        
        # Fragile items (can be detected by keywords or external input)
        # For this example, we'll use a simple check
        if weight < 5.0:
            return (3, 'Fragile')
        
        # Default to Standard
        return (1, 'Standard')
    
    def find_available_location(self, category_id: int) -> Optional[int]:
        """Find an available location for the given category."""
        self.db.cursor.execute("""
            SELECT location_id FROM Locations
            WHERE category_id = ? AND is_occupied = 0
            LIMIT 1
        """, (category_id,))
        
        result = self.db.cursor.fetchone()
        return result[0] if result else None
    
    def register_package(self, barcode: str, weight: float, length: float,
                        width: float, height: float, destination: str,
                        priority: str) -> bool:
        """Register a new package in the system."""
        try:
            # Check if barcode already exists
            self.db.cursor.execute("""
                SELECT barcode FROM Packages WHERE barcode = ?
            """, (barcode,))
            
            if self.db.cursor.fetchone():
                print(f"❌ Error: Barcode {barcode} already exists in the system!")
                return False
            
            # Categorize package
            category_id, category_name = self.categorize_package(
                weight, priority, destination
            )
            
            # Find available location
            location_id = self.find_available_location(category_id)
            
            if not location_id:
                print(f"❌ Error: No available locations for category {category_name}")
                return False
            
            # Insert package
            self.db.cursor.execute("""
                INSERT INTO Packages 
                (barcode, weight, length, width, height, destination, 
                 priority, category_id, location_id, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'Stored')
            """, (barcode, weight, length, width, height, destination, 
                  priority, category_id, location_id))
            
            package_id = self.db.cursor.lastrowid
            
            # Mark location as occupied
            self.db.cursor.execute("""
                UPDATE Locations SET is_occupied = 1 
                WHERE location_id = ?
            """, (location_id,))
            
            # Get location code
            self.db.cursor.execute("""
                SELECT location_code FROM Locations WHERE location_id = ?
            """, (location_id,))
            location_code = self.db.cursor.fetchone()[0]
            
            # Create audit trail
            self.db.cursor.execute("""
                INSERT INTO AuditTrail 
                (package_id, action, new_status, new_location, notes)
                VALUES (?, 'REGISTERED', 'Stored', ?, ?)
            """, (package_id, location_code, 
                  f"Package categorized as {category_name}"))
            
            self.db.conn.commit()
            
            print(f"✅ Package registered successfully!")
            print(f"   Barcode: {barcode}")
            print(f"   Category: {category_name}")
            print(f"   Location: {location_code}")
            
            return True
            
        except sqlite3.Error as e:
            print(f"❌ Database error: {e}")
            self.db.conn.rollback()
            return False
    
    def search_package(self, barcode: str) -> Optional[dict]:
        """Search for a package by barcode."""
        self.db.cursor.execute("""
            SELECT 
                p.package_id, p.barcode, p.weight, p.length, p.width, p.height,
                p.destination, p.priority, p.status, p.received_at,
                c.category_name, l.location_code
            FROM Packages p
            JOIN Categories c ON p.category_id = c.category_id
            LEFT JOIN Locations l ON p.location_id = l.location_id
            WHERE p.barcode = ?
        """, (barcode,))
        
        result = self.db.cursor.fetchone()
        
        if not result:
            return None
        
        return {
            'package_id': result[0],
            'barcode': result[1],
            'weight': result[2],
            'dimensions': f"{result[3]}x{result[4]}x{result[5]}",
            'destination': result[6],
            'priority': result[7],
            'status': result[8],
            'received_at': result[9],
            'category': result[10],
            'location': result[11]
        }
    
    def update_package_status(self, barcode: str, new_status: str) -> bool:
        """Update package status."""
        try:
            # Get current package info
            package = self.search_package(barcode)
            if not package:
                print(f"❌ Package {barcode} not found!")
                return False
            
            old_status = package['status']
            
            # Update status
            self.db.cursor.execute("""
                UPDATE Packages SET status = ? WHERE barcode = ?
            """, (new_status, barcode))
            
            # If status is delivered, free up location
            if new_status.lower() == 'delivered':
                self.db.cursor.execute("""
                    UPDATE Locations SET is_occupied = 0
                    WHERE location_id = (
                        SELECT location_id FROM Packages WHERE barcode = ?
                    )
                """, (barcode,))
            
            # Create audit trail
            self.db.cursor.execute("""
                INSERT INTO AuditTrail 
                (package_id, action, old_status, new_status, notes)
                VALUES (?, 'STATUS_UPDATE', ?, ?, ?)
            """, (package['package_id'], old_status, new_status,
                    f"Status changed from {old_status} to {new_status}"))
            
            self.db.conn.commit()
            print(f"✅ Package status updated: {old_status} → {new_status}")
            return True
            
        except sqlite3.Error as e:
            print(f"❌ Database error: {e}")
            self.db.conn.rollback()
            return False
    
    def get_summary_report(self) -> dict:
        """Generate summary statistics."""
        report = {}
        
        # Packages by category
        self.db.cursor.execute("""
            SELECT c.category_name, COUNT(p.package_id) as count
            FROM Categories c
            LEFT JOIN Packages p ON c.category_id = p.category_id
            GROUP BY c.category_id, c.category_name
            ORDER BY count DESC
        """)
        report['by_category'] = self.db.cursor.fetchall()
        
        # Packages by status
        self.db.cursor.execute("""
            SELECT status, COUNT(*) as count
            FROM Packages
            GROUP BY status
        """)
        report['by_status'] = self.db.cursor.fetchall()
        
        # Location occupancy
        self.db.cursor.execute("""
            SELECT 
                zone,
                COUNT(*) as total_locations,
                SUM(CASE WHEN is_occupied = 1 THEN 1 ELSE 0 END) as occupied,
                ROUND(SUM(CASE WHEN is_occupied = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as occupancy_rate
            FROM Locations
            GROUP BY zone
        """)
        report['location_occupancy'] = self.db.cursor.fetchall()
        
        # Recent activities
        self.db.cursor.execute("""
            SELECT 
                p.barcode,
                a.action,
                a.timestamp,
                a.notes
            FROM AuditTrail a
            JOIN Packages p ON a.package_id = p.package_id
            ORDER BY a.timestamp DESC
            LIMIT 10
        """)
        report['recent_activities'] = self.db.cursor.fetchall()
        
        return report


def generate_random_barcode() -> str:
    """Generate a random barcode for testing."""
    return ''.join(random.choices(string.digits, k=12))


def display_menu():
    """Display main menu."""
    print("\n" + "="*60)
    print("    DISTRIBUTION CENTER - PACKAGE MANAGEMENT SYSTEM")
    print("="*60)
    print("1. Register New Package")
    print("2. Search Package by Barcode")
    print("3. Update Package Status")
    print("4. View Summary Report")
    print("5. Generate Sample Packages (Testing)")
    print("6. Exit")
    print("="*60)


def register_package_ui(manager: PackageManager):
    """User interface for package registration."""
    print("\n--- REGISTER NEW PACKAGE ---")
    
    barcode = input("Enter barcode (or press Enter to generate random): ").strip()
    if not barcode:
        barcode = generate_random_barcode()
        print(f"Generated barcode: {barcode}")
    
    try:
        weight = float(input("Enter weight (kg): "))
        length = float(input("Enter length (cm): "))
        width = float(input("Enter width (cm): "))
        height = float(input("Enter height (cm): "))
        destination = input("Enter destination: ").strip()
        priority = input("Enter priority (Standard/Express): ").strip() or "Standard"
        
        manager.register_package(barcode, weight, length, width, height, 
                                destination, priority)
    except ValueError:
        print("❌ Invalid input! Please enter valid numbers.")


def search_package_ui(manager: PackageManager):
    """User interface for package search."""
    print("\n--- SEARCH PACKAGE ---")
    barcode = input("Enter barcode: ").strip()
    
    package = manager.search_package(barcode)
    
    if package:
        print("\n📦 Package Details:")
        print(f"   Barcode:     {package['barcode']}")
        print(f"   Category:    {package['category']}")
        print(f"   Location:    {package['location']}")
        print(f"   Weight:      {package['weight']} kg")
        print(f"   Dimensions:  {package['dimensions']} cm")
        print(f"   Destination: {package['destination']}")
        print(f"   Priority:    {package['priority']}")
        print(f"   Status:      {package['status']}")
        print(f"   Received:    {package['received_at']}")
    else:
        print(f"❌ Package with barcode {barcode} not found!")


def update_status_ui(manager: PackageManager):
    """User interface for status update."""
    print("\n--- UPDATE PACKAGE STATUS ---")
    barcode = input("Enter barcode: ").strip()
    print("\nAvailable statuses: Received, Stored, In Transit, Delivered")
    new_status = input("Enter new status: ").strip()
    
    manager.update_package_status(barcode, new_status)


def display_report(manager: PackageManager):
    """Display summary report."""
    print("\n" + "="*60)
    print("                    SUMMARY REPORT")
    print("="*60)
    
    report = manager.get_summary_report()
    
    print("\n📊 Packages by Category:")
    for category, count in report['by_category']:
        print(f"   {category:15s}: {count:3d} packages")
    
    print("\n📋 Packages by Status:")
    for status, count in report['by_status']:
        print(f"   {status:15s}: {count:3d} packages")
    
    print("\n🏢 Location Occupancy by Zone:")
    for zone, total, occupied, rate in report['location_occupancy']:
        print(f"   Zone {zone}: {occupied}/{total} ({rate}% occupied)")
    
    print("\n📝 Recent Activities:")
    for barcode, action, timestamp, notes in report['recent_activities']:
        print(f"   [{timestamp}] {barcode} - {action}")
        if notes:
            print(f"      → {notes}")


def generate_sample_packages(manager: PackageManager):
    """Generate sample packages for testing."""
    print("\n--- GENERATING SAMPLE PACKAGES ---")
    
    samples = [
        (25.0, 30, 20, 15, "New York, USA", "Express"),
        (5.0, 15, 10, 8, "Los Angeles, USA", "Standard"),
        (2.5, 10, 8, 5, "Chicago, USA", "Standard"),
        (75.0, 60, 50, 40, "Houston, USA", "Standard"),
        (15.0, 25, 20, 18, "London, UK, International", "Standard"),
        (3.0, 12, 10, 6, "Miami, USA", "Express"),
        (100.0, 80, 60, 50, "Seattle, USA", "Standard"),
    ]
    
    for weight, length, width, height, destination, priority in samples:
        barcode = generate_random_barcode()
        manager.register_package(barcode, weight, length, width, height,
                                destination, priority)
    
    print(f"\n✅ Generated {len(samples)} sample packages!")


def main():
    """Main application loop."""
    # Initialize database
    db = DistributionCenterDB()
    db.connect()
    db.initialize_database()
    
    # Create package manager
    manager = PackageManager(db)
    
    print("\n✅ Database initialized successfully!")
    
    # Main loop
    while True:
        display_menu()
        choice = input("\nEnter your choice (1-6): ").strip()
        
        if choice == '1':
            register_package_ui(manager)
        elif choice == '2':
            search_package_ui(manager)
        elif choice == '3':
            update_status_ui(manager)
        elif choice == '4':
            display_report(manager)
        elif choice == '5':
            generate_sample_packages(manager)
        elif choice == '6':
            print("\n👋 Thank you for using the Distribution Center System!")
            break
        else:
            print("❌ Invalid choice! Please enter a number between 1 and 6.")
        
        input("\nPress Enter to continue...")
    
    # Cleanup
    db.disconnect()


if __name__ == "__main__":
    main()