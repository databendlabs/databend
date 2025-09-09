#!/usr/bin/env python3
"""
Concurrent SQL test script using databend-driver.
Starts user-specified number of threads to execute SQL queries concurrently.
"""

import argparse
import threading
import time
import sys
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from databend_driver import BlockingDatabendClient


class SQLTestResult:
    """Container for SQL test results."""
    
    def __init__(self, thread_id: int, query: str):
        self.thread_id = thread_id
        self.query = query
        self.success = False
        self.error = None
        self.results = []  # Store all iteration results
        self.duration = 0.0


class ConcurrentSQLTester:
    """Manages concurrent SQL testing."""
    
    def __init__(self, servers: List[Dict[str, Any]] = None, user: str = "root", 
                 password: str = "", database: str = "default"):
        if servers is None:
            servers = [{"host": "localhost", "port": 8000}]
        self.servers = servers
        self.user = user
        self.password = password
        self.database = database
        self.results: List[SQLTestResult] = []
        self.lock = threading.Lock()
        print(f"📡 Configured {len(self.servers)} query servers:")
        for i, server in enumerate(self.servers):
            print(f"   Server {i+1}: {server['host']}:{server['port']}")
    
    def _create_connection(self, server_index: int = None) -> BlockingDatabendClient:
        """Create a new database connection to a specific server."""
        if server_index is None:
            # Round-robin server selection
            server_index = threading.current_thread().ident % len(self.servers)
        else:
            server_index = server_index % len(self.servers)
            
        server = self.servers[server_index]
        dsn = f"databend://{self.user}:{self.password}@{server['host']}:{server['port']}/{self.database}?sslmode=disable"
        return BlockingDatabendClient(dsn), server_index
    
    def _execute_query(self, thread_id: int, query: str, iterations: int = 1) -> SQLTestResult:
        """Execute SQL query in a single thread."""
        result = SQLTestResult(thread_id, query)
        
        try:
            # Use thread_id to determine server for consistent routing
            client, server_index = self._create_connection(thread_id)
            server = self.servers[server_index]
            start_time = time.time()
            
            for i in range(iterations):
                iteration_start = time.time()
                cursor = client.cursor()
                cursor.execute(query)
                
                # Fetch all results to ensure query completion
                if cursor.description:
                    rows = cursor.fetchall()
                    iteration_time = time.time() - iteration_start
                    # Convert Row objects to readable format
                    row_data = [list(row) for row in rows]
                    result.results.append({
                        'iteration': i + 1,
                        'data': row_data,
                        'duration': iteration_time,
                        'server': f"{server['host']}:{server['port']}"
                    })
                    print(f"   Thread {result.thread_id} iteration {i + 1} [Server {server_index+1}]: {row_data} ({iteration_time:.3f}s)")
                else:
                    iteration_time = time.time() - iteration_start
                    result.results.append({
                        'iteration': i + 1,
                        'data': None,
                        'duration': iteration_time,
                        'server': f"{server['host']}:{server['port']}"
                    })
                    print(f"   Thread {result.thread_id} iteration {i + 1} [Server {server_index+1}]: No result ({iteration_time:.3f}s)")
                
                cursor.close()
            
            result.duration = time.time() - start_time
            result.success = True
            
        except Exception as e:
            result.error = str(e)
            result.success = False
        
        return result
    
    def run_concurrent_test(self, query: str, num_threads: int, iterations_per_thread: int = 1) -> Dict[str, Any]:
        """Run concurrent SQL test with specified number of threads."""
        print(f"🚀 Starting concurrent SQL test:")
        print(f"   Query: {query}")
        print(f"   Threads: {num_threads}")
        print(f"   Iterations per thread: {iterations_per_thread}")
        print(f"   Total queries: {num_threads * iterations_per_thread}")
        print()
        
        start_time = time.time()
        results = []
        
        # Execute queries concurrently
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [
                executor.submit(self._execute_query, i, query, iterations_per_thread)
                for i in range(num_threads)
            ]
            
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                
                status = "✅" if result.success else "❌"
                print(f"{status} Thread {result.thread_id} completed: {result.duration:.3f}s total")
                if not result.success:
                    print(f"   Error: {result.error}")
        
        total_time = time.time() - start_time
        
        # Calculate statistics
        successful_results = [r for r in results if r.success]
        failed_results = [r for r in results if not r.success]
        
        stats = {
            "total_threads": num_threads,
            "iterations_per_thread": iterations_per_thread,
            "total_queries": num_threads * iterations_per_thread,
            "successful": len(successful_results),
            "failed": len(failed_results),
            "total_time": total_time,
            "avg_query_time": sum(r.duration for r in successful_results) / len(successful_results) if successful_results else 0,
            "queries_per_second": (len(successful_results) * iterations_per_thread) / total_time if total_time > 0 else 0
        }
        
        self._print_summary(stats, failed_results)
        return stats
    
    def _print_summary(self, stats: Dict[str, Any], failed_results: List[SQLTestResult]):
        """Print test summary."""
        print("\n📊 Test Summary:")
        print(f"   Total threads: {stats['total_threads']}")
        print(f"   Queries per thread: {stats['iterations_per_thread']}")
        print(f"   Total queries executed: {stats['total_queries']}")
        print(f"   Successful: {stats['successful']}")
        print(f"   Failed: {stats['failed']}")
        print(f"   Total time: {stats['total_time']:.3f}s")
        print(f"   Average query time: {stats['avg_query_time']:.3f}s")
        print(f"   Queries per second: {stats['queries_per_second']:.2f}")
        
        if failed_results:
            print(f"\n❌ Failed queries:")
            for result in failed_results:
                print(f"   Thread {result.thread_id}: {result.error}")


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Concurrent SQL test using databend-driver")
    parser.add_argument("--threads", "-t", type=int, default=5, 
                       help="Number of concurrent threads (default: 5)")
    parser.add_argument("--query", "-q", type=str, 
                       default="select avg(number) from yy",
                       help="SQL query to execute")
    parser.add_argument("--iterations", "-i", type=int, default=1,
                       help="Number of iterations per thread (default: 1)")
    parser.add_argument("--servers", "-s", type=str, 
                       default="localhost:8000",
                       help="Comma-separated list of servers (default: localhost:8000). Example: localhost:8000,localhost:8001")
    parser.add_argument("--user", "-u", type=str, default="ana",
                       help="Database user (default: ana)")
    parser.add_argument("--password", "-p", type=str, default="123",
                       help="Database password (default: 123)")
    parser.add_argument("--database", "-d", type=str, default="default",
                       help="Database name (default: default)")
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.threads <= 0:
        print("❌ Number of threads must be positive")
        sys.exit(1)
    
    if args.iterations <= 0:
        print("❌ Number of iterations must be positive")
        sys.exit(1)
    
    if not args.query.strip():
        print("❌ Query cannot be empty")
        sys.exit(1)
    
    # Parse servers
    servers = []
    for server_str in args.servers.split(","):
        server_str = server_str.strip()
        if ":" in server_str:
            host, port_str = server_str.split(":", 1)
            try:
                port = int(port_str)
                servers.append({"host": host.strip(), "port": port})
            except ValueError:
                print(f"❌ Invalid port in server '{server_str}'")
                sys.exit(1)
        else:
            print(f"❌ Invalid server format '{server_str}'. Expected host:port")
            sys.exit(1)
    
    # Create tester and run test
    tester = ConcurrentSQLTester(
        servers=servers,
        user=args.user,
        password=args.password,
        database=args.database
    )
    
    try:
        stats = tester.run_concurrent_test(
            query=args.query,
            num_threads=args.threads,
            iterations_per_thread=args.iterations
        )
        
        # Exit with error code if any queries failed
        if stats["failed"] > 0:
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n🔄 Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Test failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()