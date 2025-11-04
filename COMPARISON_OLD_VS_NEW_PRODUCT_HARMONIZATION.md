## Key Differences Between Old and New Product Harmonization Scripts

### MAJOR IMPROVEMENTS IN NEW VERSION:

#### 1. **EXCLUDED product_sku_base Column**

- **OLD:** Included `product_sku_base` field in product records
- **NEW:** Completely removed `product_sku_base` from processing and output
- **Impact:** Cleaner schema, no confusion with SKU handling

#### 2. **Enhanced Product Status Standardization**

- **OLD:** Basic status mapping (active/inactive)
- **NEW:** Comprehensive status mapping with platform-specific logic:
  ```python
  # Shopee status mapping
  'NORMAL': 'Active'
  'UNLIST': 'Inactive/Removed'
  'BANNED': 'Inactive/Removed'
  'SELLER_DELETE': 'Inactive/Removed'
  'SHOPEE_DELETE': 'Inactive/Removed'
  'REVIEWING': 'Pending/Reviewing'
  ```

#### 3. **Added canonical_sku to Product Variants**

- **OLD:** No unified join key for variants
- **NEW:** Added `canonical_sku` field as unified join key across platforms
- **Impact:** Enables better cross-platform variant matching

#### 4. **Implemented Lazada Special Pricing Logic**

- **OLD:** Simple price extraction from SKUs
- **NEW:** Advanced pricing logic with special price windows and date calculations
- **Features:**
  - Special price date range validation
  - Current date-based price calculations
  - Base price fallback logic

#### 5. **Smart Variant Attribute Parsing**

- **OLD:** Basic tier_index extraction
- **NEW:** Intelligent parsing that separates scent and volume:
  ```python
  # Smart assignment: separate volume from scent
  volume_pattern = re.compile(r'\d+\s*(ml|ML|mL|Ml|l|L)\b', re.IGNORECASE)
  ```
  - Volume detection from patterns like "500ml", "1L"
  - Scent assignment for non-volume attributes

#### 6. **Enhanced Rating System**

- **OLD:** No rating processing for Lazada
- **NEW:** Full rating calculation system:
  - Calculate average ratings from review data
  - Overall average rating for imputation
  - Platform-specific rating processing

#### 7. **Default Variant Creation**

- **OLD:** Only created variants from existing SKU/model data
- **NEW:** Creates default variants for ALL products (71 default variants)
- **Purpose:** Fallback variants when SKU/model_id lookup fails in fact_orders

#### 8. **Advanced Schema Management**

- **OLD:** Basic dataframe structures
- **NEW:**
  - Enhanced variant schema with more fields
  - Proper timestamp handling (created_at, last_updated)
  - Better data type enforcement

#### 9. **Improved Data Structure**

- **OLD:** Simple concatenation
- **NEW:** Structured processing with:
  - Global variant counters
  - Platform-specific key generation
  - Better error handling

### CURRENT ISSUE:

The script runs successfully but fails at the end due to a **file permission error** when trying to save CSV files. This happens when:

- Files are open in Excel/another application
- Insufficient write permissions
- Files are locked by another process

### SOLUTION:

Close any applications that might have the CSV files open, or run the script with administrator privileges.

### PERFORMANCE COMPARISON:

- **OLD:** Processed 71 products, simpler variant structure
- **NEW:** Processed 71 products + 361 variants (including 71 default variants)
- **NEW:** Better data coverage and fallback mechanisms
