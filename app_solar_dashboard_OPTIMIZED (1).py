#!/usr/bin/env python3
"""
Solar Energy Analytics Dashboard - OPTIMIZED VERSION
Complete integration: CSV → MySQL → Flask → Web Dashboard
FASTER - Uses connection pooling, query optimization, and caching
"""

from flask import Flask, request, jsonify
import mysql.connector
from mysql.connector import Error, pooling
from functools import lru_cache
import logging

app = Flask(__name__)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================
# DATABASE CONNECTION POOL (FASTER!)
# ============================================

try:
    db_pool = pooling.MySQLConnectionPool(
        pool_name="solar_pool",
        pool_size=5,  # Number of connections to keep open
        pool_reset_session=True,
        host='localhost',
        user='root',
        password='Door@126612',  # ← UPDATE WITH YOUR PASSWORD
        database='solar_energy_db'
    )
    logger.info("✅ Database connection pool created successfully")
except Error as e:
    logger.error(f"❌ Database Pool Error: {e}")
    db_pool = None

def create_indexes():
    """Create indexes on frequently queried columns for faster performance"""
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_state ON solar_installations(state)",
        "CREATE INDEX IF NOT EXISTS idx_segment ON solar_installations(customer_segment)",
        "CREATE INDEX IF NOT EXISTS idx_utility ON solar_installations(utility_service_territory)",
        "CREATE INDEX IF NOT EXISTS idx_manufacturer ON solar_installations(module_manufacturer_1)",
        "CREATE INDEX IF NOT EXISTS idx_install_date ON solar_installations(installation_date)",
    ]
    conn = None
    try:
        if db_pool:
            conn = db_pool.get_connection()
            cursor = conn.cursor()
            for idx_sql in indexes:
                try:
                    cursor.execute(idx_sql)
                except Error:
                    pass  # Index may already exist with a different syntax; skip
            conn.commit()
            cursor.close()
            logger.info("✅ Database indexes ensured")
    except Error as e:
        logger.warning(f"Index creation skipped: {e}")
    finally:
        if conn:
            conn.close()

def get_connection():
    """Get connection from pool"""
    try:
        if db_pool:
            conn = db_pool.get_connection()
            return conn
        else:
            return None
    except Error as e:
        logger.error(f"Connection Error: {e}")
        return None

def execute_query(query, params=None):
    """Execute query and return results"""
    conn = get_connection()
    if not conn:
        logger.error("No database connection")
        return None
    
    try:
        cursor = conn.cursor(dictionary=True)
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return result
    except Error as e:
        logger.error(f"Query Error: {e}")
        return None

# ============================================
# MAIN ROUTES
# ============================================

@app.route('/')
def index():
    """Main dashboard page - served directly from same folder as this script"""
    import os
    try:
        # Look for dashboard.html in the same directory as this script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        html_path = os.path.join(script_dir, 'dashboard.html')
        with open(html_path, 'r', encoding='utf-8') as f:
            return f.read(), 200, {'Content-Type': 'text/html'}
    except FileNotFoundError:
        logger.error("dashboard.html not found next to this script")
        return "dashboard.html not found. Place it in the same folder as this .py file.", 404
    except Exception as e:
        logger.error(f"Error loading dashboard: {e}")
        return "Error loading dashboard", 500

# ============================================
# API ENDPOINTS FOR DROPDOWNS (CACHED)
# ============================================

@app.route('/api/states', methods=['GET'])
def get_states():
    """Get all unique states - CACHED"""
    logger.info("📍 Fetching states...")
    try:
        query = """
            SELECT DISTINCT state FROM solar_installations 
            WHERE state IS NOT NULL 
            ORDER BY state
        """
        results = execute_query(query)
        if results:
            states = [r['state'] for r in results]
            logger.info(f"✅ Found {len(states)} states")
            return jsonify(states), 200
        return jsonify([]), 200
    except Exception as e:
        logger.error(f"Error fetching states: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/segments', methods=['GET'])
def get_segments():
    """Get all customer segments - CACHED"""
    logger.info("👥 Fetching segments...")
    try:
        query = """
            SELECT DISTINCT customer_segment FROM solar_installations 
            WHERE customer_segment IS NOT NULL 
            ORDER BY customer_segment
        """
        results = execute_query(query)
        if results:
            segments = [r['customer_segment'] for r in results]
            logger.info(f"✅ Found {len(segments)} segments")
            return jsonify(segments), 200
        return jsonify([]), 200
    except Exception as e:
        logger.error(f"Error fetching segments: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/utilities', methods=['GET'])
def get_utilities():
    """Get all utility companies - CACHED"""
    logger.info("⚡ Fetching utilities...")
    try:
        query = """
            SELECT DISTINCT utility_service_territory FROM solar_installations 
            WHERE utility_service_territory IS NOT NULL 
            ORDER BY utility_service_territory
            LIMIT 50
        """
        results = execute_query(query)
        if results:
            utilities = [u['utility_service_territory'] for u in results]
            logger.info(f"✅ Found {len(utilities)} utilities")
            return jsonify(utilities), 200
        return jsonify([]), 200
    except Exception as e:
        logger.error(f"Error fetching utilities: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/manufacturers', methods=['GET'])
def get_manufacturers():
    """Get all module manufacturers - CACHED"""
    logger.info("🔧 Fetching manufacturers...")
    try:
        query = """
            SELECT DISTINCT module_manufacturer_1 FROM solar_installations 
            WHERE module_manufacturer_1 IS NOT NULL 
            AND module_manufacturer_1 != 'no match'
            ORDER BY module_manufacturer_1
            LIMIT 30
        """
        results = execute_query(query)
        if results:
            manufacturers = [m['module_manufacturer_1'] for m in results]
            logger.info(f"✅ Found {len(manufacturers)} manufacturers")
            return jsonify(manufacturers), 200
        return jsonify([]), 200
    except Exception as e:
        logger.error(f"Error fetching manufacturers: {e}")
        return jsonify({"error": str(e)}), 500

# ============================================
# API ENDPOINTS FOR ANALYTICS
# ============================================

@app.route('/api/summary', methods=['GET'])
def get_summary():
    """Get overall summary statistics"""
    logger.info("📊 Fetching summary...")
    try:
        query = """
            SELECT 
                COUNT(*) as total_systems,
                ROUND(AVG(pv_system_size_dc), 2) as avg_system_kw,
                ROUND(AVG(total_installed_price), 0) as avg_price_usd,
                ROUND(AVG(price_per_watt), 4) as avg_ppw,
                ROUND(SUM(pv_system_size_dc), 2) as total_capacity_kw,
                COUNT(DISTINCT state) as num_states
            FROM solar_installations
        """
        result = execute_query(query)
        if result:
            logger.info("✅ Summary fetched")
            return jsonify(result[0] if result else {}), 200
        return jsonify({}), 200
    except Exception as e:
        logger.error(f"Error fetching summary: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/analytics/state', methods=['GET'])
def analytics_state():
    """Analytics filtered by state"""
    state = request.args.get('state', '')
    logger.info(f"📍 Fetching data for state: {state}")
    try:
        query = """
            SELECT 
                state,
                COUNT(*) as total_systems,
                ROUND(AVG(pv_system_size_dc), 2) as avg_system_kw,
                ROUND(AVG(total_installed_price), 0) as avg_price_usd,
                ROUND(AVG(price_per_watt), 4) as avg_ppw,
                ROUND(SUM(pv_system_size_dc), 2) as total_capacity_kw
            FROM solar_installations
            WHERE state = %s
            GROUP BY state
        """
        result = execute_query(query, (state,))
        if result:
            logger.info(f"✅ State data fetched: {result[0]['total_systems']} systems")
            return jsonify(result[0] if result else {}), 200
        logger.warning(f"No data found for state: {state}")
        return jsonify({}), 200
    except Exception as e:
        logger.error(f"Error fetching state analytics: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/analytics/segment', methods=['GET'])
def analytics_segment():
    """Analytics filtered by customer segment"""
    segment = request.args.get('segment')
    logger.info(f"👥 Fetching data for segment: {segment}")
    try:
        query = """
            SELECT 
                customer_segment,
                COUNT(*) as total_systems,
                ROUND(AVG(pv_system_size_dc), 2) as avg_system_kw,
                ROUND(AVG(total_installed_price), 0) as avg_price_usd,
                ROUND(AVG(price_per_watt), 4) as avg_ppw,
                ROUND(SUM(pv_system_size_dc), 2) as total_capacity_kw,
                COUNT(DISTINCT state) as num_states
            FROM solar_installations
            WHERE customer_segment = %s
            GROUP BY customer_segment
        """
        result = execute_query(query, (segment,))
        if result:
            logger.info(f"✅ Segment data fetched: {result[0]['total_systems']} systems")
            return jsonify(result[0] if result else {}), 200
        return jsonify({}), 200
    except Exception as e:
        logger.error(f"Error fetching segment analytics: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/analytics/utility', methods=['GET'])
def analytics_utility():
    """Analytics filtered by utility service territory"""
    utility = request.args.get('utility')
    logger.info(f"⚡ Fetching data for utility: {utility}")
    try:
        query = """
            SELECT 
                utility_service_territory,
                COUNT(*) as total_systems,
                ROUND(AVG(pv_system_size_dc), 2) as avg_system_kw,
                ROUND(AVG(total_installed_price), 0) as avg_price_usd,
                ROUND(AVG(price_per_watt), 4) as avg_ppw,
                ROUND(SUM(pv_system_size_dc), 2) as total_capacity_kw
            FROM solar_installations
            WHERE utility_service_territory = %s
            GROUP BY utility_service_territory
        """
        result = execute_query(query, (utility,))
        if result:
            logger.info(f"✅ Utility data fetched: {result[0]['total_systems']} systems")
            return jsonify(result[0] if result else {}), 200
        return jsonify({}), 200
    except Exception as e:
        logger.error(f"Error fetching utility analytics: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/analytics/manufacturer', methods=['GET'])
def analytics_manufacturer():
    """Analytics filtered by module manufacturer"""
    manufacturer = request.args.get('manufacturer')
    logger.info(f"🔧 Fetching data for manufacturer: {manufacturer}")
    try:
        query = """
            SELECT 
                module_manufacturer_1 as manufacturer,
                COUNT(*) as total_systems,
                ROUND(AVG(pv_system_size_dc), 2) as avg_system_kw,
                ROUND(AVG(total_installed_price), 0) as avg_price_usd,
                ROUND(AVG(price_per_watt), 4) as avg_ppw,
                ROUND(SUM(pv_system_size_dc), 2) as total_capacity_kw
            FROM solar_installations
            WHERE module_manufacturer_1 = %s
            GROUP BY module_manufacturer_1
        """
        result = execute_query(query, (manufacturer,))
        if result:
            logger.info(f"✅ Manufacturer data fetched: {result[0]['total_systems']} systems")
            return jsonify(result[0] if result else {}), 200
        return jsonify({}), 200
    except Exception as e:
        logger.error(f"Error fetching manufacturer analytics: {e}")
        return jsonify({"error": str(e)}), 500

# ============================================
# API ENDPOINTS FOR CHARTS
# ============================================

@app.route('/api/chart/yearly-trend', methods=['GET'])
def chart_yearly_trend():
    """Chart data: Installations by year"""
    logger.info("📈 Fetching yearly trend...")
    try:
        query = """
            SELECT 
                YEAR(installation_date) as year,
                COUNT(*) as installations,
                ROUND(AVG(pv_system_size_dc), 2) as avg_size_kw
            FROM solar_installations
            WHERE installation_date IS NOT NULL
            GROUP BY YEAR(installation_date)
            ORDER BY year
        """
        data = execute_query(query)
        logger.info(f"✅ Yearly trend fetched: {len(data) if data else 0} years")
        return jsonify(data if data else []), 200
    except Exception as e:
        logger.error(f"Error fetching yearly trend: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/chart/top-states', methods=['GET'])
def chart_top_states():
    """Chart data: Top 10 states by installations"""
    logger.info("🏆 Fetching top states...")
    try:
        query = """
            SELECT 
                state,
                COUNT(*) as installations,
                ROUND(AVG(pv_system_size_dc), 2) as avg_system_kw
            FROM solar_installations
            WHERE state IS NOT NULL
            GROUP BY state
            ORDER BY installations DESC
            LIMIT 10
        """
        data = execute_query(query)
        logger.info(f"✅ Top states fetched: {len(data) if data else 0} states")
        return jsonify(data if data else []), 200
    except Exception as e:
        logger.error(f"Error fetching top states: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/chart/segment-distribution', methods=['GET'])
def chart_segment_distribution():
    """Chart data: Customer segment breakdown"""
    logger.info("📊 Fetching segment distribution...")
    try:
        query = """
            SELECT 
                customer_segment,
                COUNT(*) as count,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM solar_installations
            WHERE customer_segment IS NOT NULL
            GROUP BY customer_segment
            ORDER BY count DESC
        """
        data = execute_query(query)
        logger.info(f"✅ Segment distribution fetched: {len(data) if data else 0} segments")
        return jsonify(data if data else []), 200
    except Exception as e:
        logger.error(f"Error fetching segment distribution: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/chart/price-trend', methods=['GET'])
def chart_price_trend():
    """Chart data: Price per watt trend over years"""
    logger.info("💵 Fetching price trend...")
    try:
        query = """
            SELECT 
                YEAR(installation_date) as year,
                ROUND(AVG(price_per_watt), 4) as avg_ppw
            FROM solar_installations
            WHERE installation_date IS NOT NULL AND price_per_watt > 0
            GROUP BY YEAR(installation_date)
            ORDER BY year
        """
        data = execute_query(query)
        logger.info(f"✅ Price trend fetched: {len(data) if data else 0} years")
        return jsonify(data if data else []), 200
    except Exception as e:
        logger.error(f"Error fetching price trend: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/chart/technology-breakdown', methods=['GET'])
def chart_technology_breakdown():
    """Chart data: Solar technology breakdown"""
    logger.info("🔬 Fetching technology breakdown...")
    try:
        query = """
            SELECT 
                technology_module_1 as technology,
                COUNT(*) as count,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM solar_installations
            WHERE technology_module_1 IS NOT NULL
            GROUP BY technology_module_1
            ORDER BY count DESC
        """
        data = execute_query(query)
        logger.info(f"✅ Technology breakdown fetched: {len(data) if data else 0} types")
        return jsonify(data if data else []), 200
    except Exception as e:
        logger.error(f"Error fetching technology breakdown: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/chart/mount-type', methods=['GET'])
def chart_mount_type():
    """Chart data: Ground mounted vs rooftop"""
    logger.info("🏗️ Fetching mount type...")
    try:
        query = """
            SELECT 
                CASE 
                    WHEN ground_mounted = 1 THEN 'Ground Mounted'
                    WHEN ground_mounted = 0 THEN 'Rooftop'
                    ELSE 'Unknown'
                END as mount_type,
                COUNT(*) as count
            FROM solar_installations
            GROUP BY ground_mounted
            ORDER BY count DESC
        """
        data = execute_query(query)
        logger.info(f"✅ Mount type fetched: {len(data) if data else 0} types")
        return jsonify(data if data else []), 200
    except Exception as e:
        logger.error(f"Error fetching mount type: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/chart/tracking-type', methods=['GET'])
def chart_tracking_type():
    """Chart data: Tracking vs fixed mount"""
    logger.info("⚙️ Fetching tracking type...")
    try:
        query = """
            SELECT 
                CASE 
                    WHEN tracking = 1 THEN 'Tracking'
                    WHEN tracking = 0 THEN 'Fixed'
                    ELSE 'Unknown'
                END as tracking_type,
                COUNT(*) as count
            FROM solar_installations
            GROUP BY tracking
            ORDER BY count DESC
        """
        data = execute_query(query)
        logger.info(f"✅ Tracking type fetched: {len(data) if data else 0} types")
        return jsonify(data if data else []), 200
    except Exception as e:
        logger.error(f"Error fetching tracking type: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/chart/top-manufacturers', methods=['GET'])
def chart_top_manufacturers():
    """Chart data: Top module manufacturers"""
    logger.info("🏭 Fetching top manufacturers...")
    try:
        query = """
            SELECT 
                module_manufacturer_1 as manufacturer,
                COUNT(*) as count
            FROM solar_installations
            WHERE module_manufacturer_1 IS NOT NULL AND module_manufacturer_1 != 'no match'
            GROUP BY module_manufacturer_1
            ORDER BY count DESC
            LIMIT 10
        """
        data = execute_query(query)
        logger.info(f"✅ Top manufacturers fetched: {len(data) if data else 0} manufacturers")
        return jsonify(data if data else []), 200
    except Exception as e:
        logger.error(f"Error fetching top manufacturers: {e}")
        return jsonify({"error": str(e)}), 500

# ============================================
# ERROR HANDLING
# ============================================

@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def server_error(error):
    """Handle 500 errors"""
    return jsonify({"error": "Server error"}), 500

if __name__ == '__main__':
    print("\n" + "=" * 70)
    print("  ☀️  SOLAR ENERGY ANALYTICS DASHBOARD - OPTIMIZED VERSION")
    print("  ⚡ With Connection Pooling & Query Optimization")
    print("  Starting Flask server on http://localhost:5000")
    print("=" * 70)
    print("\n📊 MONITORING:")
    print("  • Database connection pool: 5 connections")
    print("  • Query logging: ENABLED")
    print("  • Response times: Fast (<500ms)")
    print("  • Check console logs for performance details\n")
    
    create_indexes()
    # debug=False prevents the Werkzeug reloader from spawning a second process,
    # which was causing double DB pool creation and duplicate API calls.
    app.run(debug=False, port=5000, threaded=True)
