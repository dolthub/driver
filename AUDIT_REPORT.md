# Driver vs Dolt Main Audit Report

**Date:** 2026-01-15
**Auditor:** Polecat Furiosa
**Task ID:** dr-brz

## Executive Summary

This audit compares the current `dolthub/driver` implementation against the main `dolthub/dolt` repository to identify API changes, new features, and breaking changes that need to be incorporated.

**Key Findings:**
- Driver is up-to-date with upstream main branch (as of Sep 16, 2025)
- Driver dependencies are current (Sep 2025 snapshot of Dolt)
- No breaking API changes identified in driver itself
- All Dolt SQL features are accessible through the driver
- One open PR pending (#36) for relative path support
- Dolt main has continued evolving with new features since driver's dependency freeze

---

## Current State Analysis

### Driver Repository Status
- **Current Branch:** polecat/furiosa-mkftk535
- **Upstream Status:** In sync with dolthub/driver main (commit 59e5e6a)
- **Last Release:** v0.2.0 (July 11, 2024)
- **Last Commit:** Sep 16, 2025 - "Merge pull request #38 from dolthub/aaron/icu-cgo-dolt-dep"
- **Open Issues:** 0
- **Open PRs:** 1 (#36 - relative path support)

### Dependency Versions
```
github.com/dolthub/dolt/go v0.40.5-0.20250916084833-f121f4db6fdd
github.com/dolthub/go-mysql-server v0.20.1-0.20250916073142-59b34ad42ad7
github.com/dolthub/vitess v0.0.0-20250915221346-753c44800850
```

**Dependency Date:** September 16, 2025 (commit f121f4db6fdd)

### Driver API Surface
The driver exposes the following through Go's `database/sql` interface:

**1. Connection Management**
- `driver.Driver` implementation (doltDriver)
- `driver.Conn` implementation (DoltConn)
- DSN format: `file:///path/to/dbs?params`

**2. DSN Parameters**
- `commitname` - Committer name for dolt log
- `commitemail` - Committer email for dolt log
- `database` - Initial database to connect to
- `multistatements` - Enable multi-statement queries
- `clientfoundrows` - Return matching rows vs changed rows

**3. SQL Operations**
- Statement preparation (single and multi-statement)
- Query execution
- Transaction support (BEGIN, COMMIT, ROLLBACK)
- Result iteration with multi-result set support

**4. Type Support**
- Standard SQL types
- Geometry types (GeometryValue serialization)
- Enum types
- Set types
- driver.Valuer interface support

**5. Error Handling**
- Translation to MySQL-compatible errors
- Full error context preservation

---

## Dolt Main Repository Analysis

### Latest Dolt Release
- **Version:** v1.80.1 (January 10, 2025)
- **Previous Versions:** v1.80.0, v1.79.4, v1.79.3, v1.79.2, v1.79.1, v1.79.0, v1.78.8, v1.78.7, v1.78.6

### Key Features Available in Dolt (All Accessible via Driver)

**Version Control SQL Functions:**
- `DOLT_DIFF()` - Calculate differences between commits
- `DOLT_PATCH()` - Generate SQL patch statements
- `DOLT_PREVIEW_MERGE_CONFLICTS_SUMMARY()` - Preview merge conflicts
- `DOLT_COMMIT_DIFF_*()` - Diff specific tables across commits
- Additional diff and history functions

**Version Control SQL Procedures:**
- `DOLT_COMMIT()` - Create commits
- `DOLT_CHECKOUT()` - Switch branches
- `DOLT_MERGE()` - Merge branches
- `DOLT_BRANCH()` - Manage branches
- `DOLT_TAG()` - Manage tags
- `DOLT_RESET()` - Reset working set
- `DOLT_REVERT()` - Revert commits
- Many others for complete Git-like operations

**System Tables:**
- `dolt_log` - Commit history
- `dolt_diff_*` - Table-specific diffs
- `dolt_commits` - Commit metadata
- `dolt_branches` - Branch information
- `dolt_status` - Working set status
- Many others for version control metadata

---

## Feature Comparison

### ✅ Features Fully Supported

All Dolt version control features are **fully accessible** through the driver because:

1. **SQL Interface:** The driver provides standard `database/sql` connectivity
2. **Transparent Access:** All Dolt SQL functions, procedures, and system tables work through normal SQL queries
3. **Embedded Engine:** Uses `engine.SqlEngine` which exposes complete Dolt functionality

**Example Usage Through Driver:**
```go
// All of these work transparently through the driver
db.Exec("CALL DOLT_COMMIT('-am', 'commit message')")
db.Query("SELECT * FROM dolt_log")
db.Query("SELECT * FROM DOLT_DIFF('branch1', 'branch2', 'tablename')")
db.Exec("CALL DOLT_CHECKOUT('-b', 'new-branch')")
```

### 📋 Recent Dolt Improvements (Since Sep 2025 Dependency)

The following features were added to Dolt main **after** the driver's dependency freeze:

**v1.80.1 (Jan 2025)**
- Bug fixes for stored procedures with `IS` expressions
- `NATURAL FULL JOIN` parsing fixes

**v1.80.0 (Jan 2025)**
- ⚠️ **Breaking Change:** CLI stricter about repository requirements
- `DROP SCHEMA` support added
- Note: CLI changes don't affect embedded driver

**v1.79.4 (Jan 2025)**
- Fix for duplicate table display in `SHOW TABLES`

**v1.79.3 (Dec 2024)**
- Schema override support for `dolt_diff()`
- Environment variable interpolation in configuration

**v1.79.2 (Dec 2024)**
- Performance optimizations
- `merge --ff-only` flag support

**v1.79.1 (Dec 2024)**
- Doltgres override improvements
- Trigger functionality enhancements

**v1.79.0 (Dec 2024)**
- JWT support for metrics
- Table adapter functionality

**v1.78.8 - v1.78.6 (Dec 2024)**
- Memory optimizations for AWS remote backups
- JSON merge conflict fixes
- Datetime precision improvements
- Check constraint fixes
- RPM build artifacts added

---

## Gap Analysis

### 1. Dependency Version Gap

**Current Situation:**
- Driver pins Dolt to September 16, 2025 snapshot
- Dolt main has releases through January 10, 2025
- Driver is ~4 months behind latest Dolt features

**Impact:** Low to Medium
- Most improvements are bug fixes and optimizations
- No critical features missing for embedded use case
- New SQL features automatically available once dependency updated

**Recommendation:**
```
Bump dependency to latest stable Dolt release (v1.80.1 or later)
Update go.mod:
  github.com/dolthub/dolt/go v1.80.1
  github.com/dolthub/go-mysql-server (matching version)
```

### 2. Open Pull Request

**PR #36: Fix DB Open with relative paths**
- Author: dennwc
- Created: August 23, 2025
- Status: Not merged

**Impact:** Low
- Quality of life improvement
- Doesn't block core functionality
- Users can use absolute paths as workaround

**Recommendation:** Review and merge if tests pass

### 3. Missing Driver-Specific Features

**None Identified**

The driver correctly exposes:
- ✅ Multi-statement support
- ✅ Client found rows capability
- ✅ Transaction isolation levels
- ✅ Proper error translation
- ✅ Type conversions (Enum, Set, Geometry)
- ✅ Context support
- ✅ Connection configuration

### 4. Configuration Options

**Currently Exposed:**
- Directory path (via DSN)
- Commit name/email (via DSN)
- Database selection (via DSN)
- Multi-statements flag (via DSN)
- Client found rows flag (via DSN)

**Potentially Missing (from SqlEngine):**
- Read-only mode configuration
- Server user configuration
- Autocommit configuration
- Custom version string

**Current Implementation:**
```go
// driver.go:86-90
seCfg := &engine.SqlEngineConfig{
    IsReadOnly: false,        // Hardcoded
    ServerUser: "root",       // Hardcoded
    Autocommit: true,         // Hardcoded
}
```

**Impact:** Low
- Current defaults are sensible
- Most users won't need to change these
- Can be extended via DSN if needed

**Recommendation:** Consider adding optional DSN parameters:
- `readonly=true` - Enable read-only mode
- `user=username` - Set server user
- `autocommit=false` - Disable autocommit

---

## Breaking Changes

### None Identified in Driver API

The driver maintains full backwards compatibility:
- No public API changes
- DSN format unchanged
- Behavior consistent with previous versions

### Dolt v1.80.0 Breaking Change

**Change:** CLI stricter about repository requirements

**Impact on Driver:** None
- This affects only CLI usage
- Embedded driver bypasses CLI entirely
- Uses `engine.SqlEngine` directly

---

## API Differences from Dolt Main

### What Driver Exposes vs. What Dolt CLI Provides

| Feature | Dolt CLI | Embedded Driver |
|---------|----------|-----------------|
| SQL Operations | ✅ via `dolt sql` | ✅ Native |
| Version Control | ✅ via `dolt commit`, etc. | ✅ via SQL procedures |
| Schema Changes | ✅ via SQL | ✅ via SQL |
| Remote Operations | ✅ via `dolt push/pull` | ✅ via SQL procedures |
| Configuration | ✅ via `dolt config` | ⚠️ Limited (DSN params) |
| Init New DB | ✅ via `dolt init` | ✅ via `CREATE DATABASE` |
| Repository Mgmt | ✅ via CLI | ⚠️ Limited access |

**Key Difference:**
- CLI provides rich command-line experience
- Driver provides programmatic SQL-based access
- Both access same underlying engine
- Driver is focused on SQL operations

---

## Security & Safety

### Current Implementation Review

**✅ Good Practices:**
1. Error translation prevents information leakage
2. Prepared statements support parameter binding
3. Transaction isolation properly handled
4. Context support for cancellation
5. Proper resource cleanup (Close methods)

**⚠️ Considerations:**
1. Commit credentials hardcoded in DSN (visible in connection strings)
2. No authentication/authorization layer (relies on filesystem permissions)
3. Multi-user scenarios may have conflicts (session-based branches)

**No Critical Security Issues Identified**

---

## Performance Considerations

### Driver-Specific Optimizations

**✅ Implemented:**
1. Lazy result set evaluation (peekableRowIter)
2. Sequential query execution for multi-statements
3. Context reuse across statements
4. Schema caching in rows iterator

**Recent Dolt Improvements Available:**
1. Memory optimizations for remote operations (v1.78.8)
2. Row estimate improvements (v1.79.2)
3. General performance optimizations

**Recommendation:** Update dependency to pick up performance improvements

---

## Compatibility Analysis

### Go Module Compatibility

**Current:**
```
go 1.24.6
toolchain go1.24.7
```

**Status:** Modern and well-supported

### Database/SQL Compliance

**Interfaces Implemented:**
- ✅ `driver.Driver`
- ✅ `driver.Conn`
- ✅ `driver.Stmt`
- ✅ `driver.Rows`
- ✅ `driver.Result`
- ✅ `driver.Tx`
- ✅ `driver.RowsNextResultSet` (for multi-statements)

**Missing Optional Interfaces:**
- `driver.ConnBeginTx` - Actually implemented ✅
- `driver.ConnPrepareContext` - Could be added
- `driver.StmtExecContext` - Could be added
- `driver.StmtQueryContext` - Could be added

**Impact:** Low
- Current implementation is sufficient
- Optional interfaces would improve context propagation

---

## Testing & Quality

### Test Coverage

**Current Test Files:**
- `data_source_test.go` - DSN parsing
- `errors_test.go` - Error translation
- `gorm_test.go` - GORM integration
- `query_splitter_test.go` - Query splitting
- `smoke_test.go` - End-to-end tests

**Recent Driver Changes:**
- Multi-result set handling (Jul 2025)
- ICU4C Cgo dependency (Sep 2025)
- CI/CD improvements for cross-platform testing

**Quality Indicators:**
- ✅ Zero open issues
- ✅ Active maintenance
- ✅ Cross-platform CI
- ✅ Integration tests

---

## Recommendations

### Priority 1: Immediate Actions

1. **Merge Open PR #36**
   - Enables relative path support
   - Low risk, high convenience
   - Action: Review and merge

2. **Update Documentation**
   - Document all DSN parameters
   - Add examples for version control operations
   - Clarify multi-statement behavior

### Priority 2: Short-term Improvements (1-2 weeks)

1. **Bump Dolt Dependency**
   ```
   Update to latest stable release (v1.80.1+)
   Test compatibility
   Cut new driver release (v0.3.0)
   ```

2. **Enhance Configuration Options**
   ```go
   Add optional DSN parameters:
   - readonly=true
   - user=username
   - autocommit=false
   ```

3. **Implement Optional Context Interfaces**
   ```go
   - driver.ConnPrepareContext
   - driver.StmtExecContext
   - driver.StmtQueryContext
   ```

### Priority 3: Long-term Enhancements (Future)

1. **Configuration Management**
   - Consider config file support
   - Environment variable support
   - More granular engine configuration

2. **Connection Pooling Optimization**
   - Investigate session-level state management
   - Branch context tracking
   - Transaction context improvements

3. **Enhanced Examples**
   - Version control workflows
   - Branch management patterns
   - Merge conflict resolution
   - Integration with popular ORMs

4. **Performance Monitoring**
   - Add optional metrics
   - Query profiling support
   - Connection pool statistics

### Non-Recommendations (Things NOT to Change)

1. ❌ Don't change DSN format (breaking change)
2. ❌ Don't add CLI-like features (out of scope)
3. ❌ Don't wrap SQL procedures (unnecessary abstraction)
4. ❌ Don't add custom version control API (SQL is sufficient)

---

## Conclusion

### Overall Assessment: ✅ HEALTHY

The `dolthub/driver` repository is in good shape:

**Strengths:**
- Clean, focused implementation
- Good test coverage
- Zero open issues
- Active maintenance
- Full Dolt feature access

**Areas for Improvement:**
- Dependency updates
- Configuration flexibility
- Enhanced documentation

**Critical Issues:** None

**Breaking Changes Required:** None

### Sync Requirements

**From Dolt Main to Driver:**
- ✅ No API changes needed
- ✅ No breaking changes to incorporate
- ⚠️ Dependency bump recommended (not required)
- ⚠️ Bug fixes available in newer Dolt versions

**Action Items:**
1. Bump `github.com/dolthub/dolt/go` to v1.80.1+
2. Merge PR #36 (relative paths)
3. Cut release v0.3.0
4. Update documentation

### Next Steps

1. **Immediate (This Week):**
   - Review PR #36
   - Plan dependency bump

2. **Short-term (This Month):**
   - Update dependencies
   - Run full test suite
   - Cut new release

3. **Long-term (Next Quarter):**
   - Enhance configuration
   - Improve documentation
   - Add more examples

---

## References

- [Dolt GitHub Repository](https://github.com/dolthub/dolt)
- [Driver GitHub Repository](https://github.com/dolthub/driver)
- [Dolt Documentation](https://docs.dolthub.com/)
- [Version Control Features](https://docs.dolthub.com/sql-reference/version-control)
- [Dolt Releases](https://github.com/dolthub/dolt/releases)
- [Embedding Dolt Blog Post](https://www.dolthub.com/blog/2022-07-25-embedded/)

---

**Audit Complete**
**Status:** No critical issues found
**Risk Level:** Low
**Recommendation:** Proceed with incremental improvements
