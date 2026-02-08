#!/bin/bash

# ==============================================================================
# Settings & Configuration
# ==============================================================================
SEARCH_PATH="."
RECURSIVE=false
DRY_RUN=false
KEEP=""
MODE="symlink"
ALGORITHM="md5"
IGNORE="symlink,.lnk,.url"
THREADS=$(nproc 2>/dev/null || echo 4)

# ==============================================================================
# Helper & Logging Functions
# ==============================================================================
usage() {
    echo "Usage: $0 -k <keep> [-p <path>] [-r] [-d] [-m <mode>] [-a <algo>] [-i <ignore>] [-t <threads>]"
    echo "Options:"
    echo "  -p, --path         Search directory (default: .)"
    echo "  -r, --recursive    Recursive search"
    echo "  -d, --dry-run      Simulation mode"
    echo "  -k, --keep         Criteria: latest, oldest, highest, deepest, first, last"
    echo "  -m, --mode         Action: lnk, symlink, hardlink, delete (default: symlink)"
    echo "  -a, --algorithm    Algorithm: name, size, crc32, md5, sha256, sha512 (default: md5)"
    echo "  -i, --ignore       Comma-separated ignore list (default: symlink,.lnk,.url)"
    echo "  -t, --threads      Parallel hashing threads (default: CPU count)"
    exit 1
}

log_msg() {
    local color=$2
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "[$timestamp] $1" | tee -a "$LOG_FILE"
}

get_disk_space_raw() {
    local stats=$(df -Pk "$ABS_PATH" 2>/dev/null | tail -n 1)
    [[ -z "$stats" ]] && return 1
    echo "$stats" | awk '{print $2, $4}'
}

format_disk_info() {
    local total_kb=$1
    local free_kb=$2
    [[ -z "$total_kb" || "$total_kb" -eq 0 ]] && { echo "Unknown"; return; }
    local total_gb=$(( total_kb / 1024 / 1024 ))
    local free_gb=$(( free_kb / 1024 / 1024 ))
    local percent=$(( 100 * free_kb / total_kb ))
    echo "$free_gb/${total_gb}GB ($percent%)"
}

# ==============================================================================
# CSV & Cache Functions
# ==============================================================================
validate_csv_line() {
    local line="$1"
    [[ $(echo "$line" | tr -cd ';' | wc -c) -ne 4 ]] && return 1
    local c_path c_size c_time c_type c_hash
    IFS=';' read -r c_path c_size c_time c_type c_hash <<< "$line"
    [[ -z "$c_path" || ! -f "$ABS_PATH/$c_path" ]] && return 1
    [[ ! "$c_size" =~ ^[0-9]+$ || ! "$c_time" =~ ^[0-9]+$ ]] && return 1
    case "$c_type" in
        md5|sha256|sha512|crc32|size|name) ;;
        *) return 1 ;;
    esac
    case "$c_type" in
        md5)    [[ ! "$c_hash" =~ ^[a-fA-F0-9]{32}$ ]] && return 1 ;;
        sha256) [[ ! "$c_hash" =~ ^[a-fA-F0-9]{64}$ ]] && return 1 ;;
        sha512) [[ ! "$c_hash" =~ ^[a-fA-F0-9]{128}$ ]] && return 1 ;;
        crc32)  [[ ! "$c_hash" =~ ^[a-fA-F0-9]{8}$ ]] && return 1 ;;
    esac
    return 0
}

load_cache() {
    if [[ -f "$HASH_CSV" ]]; then
        log_msg "Loading hash cache from $HASH_CSV..."
        local corrupt=0
        while IFS= read -r line; do
            [[ "$line" == "path;size;time;type;hash" || -z "$line" ]] && continue
            if validate_csv_line "$line"; then
                IFS=';' read -r c_path c_size c_time c_type c_hash <<< "$line"
                HASH_CACHE["$c_path|$c_size|$c_time|$c_type"]="$c_hash"
            else
                corrupt=$((corrupt + 1))
            fi
        done < "$HASH_CSV"
        [[ $corrupt -gt 0 ]] && log_msg "  Ignored $corrupt corrupt/invalid lines in cache." "Yellow"
    else
        echo "path;size;time;type;hash" > "$HASH_CSV"
    fi
}

save_hash_entry() {
    echo "$1;$2;$3;$4;$5" >> "$HASH_CSV"
}

# ==============================================================================
# Discovery & Discovery Functions
# ==============================================================================
get_files() {
    local find_opts=""
    IFS=',' read -ra IGNORES <<< "$IGNORE"
    for p in "${IGNORES[@]}"; do
        [[ "$p" == "symlink" ]] && continue
        find_opts+=" ! -name \"*$p\""
    done
    local cmd="find \"$ABS_PATH\" -type f $find_opts"
    [[ "$RECURSIVE" == false ]] && cmd+=" -maxdepth 1"
    eval "$cmd -print0"
}

filter_hardlinks() {
    local count=0
    declare -A seen_inodes
    while IFS= read -r -d '' file; do
        count=$((count + 1))
        (( count % 1000 == 0 )) && printf "  Found %d entries...\r" "$count" >&2
        local inode=$(stat -c %i "$file" 2>/dev/null)
        if [[ -n "$inode" && -z "${seen_inodes[$inode]}" ]]; then
            seen_inodes["$inode"]=1
            echo "$file"
        fi
    done
    echo "" >&2
}

# ==============================================================================
# Hashing & Grouping Functions
# ==============================================================================
get_single_hash() {
    local algo="$1"
    local file="$2"
    local prog="md5sum"
    [[ "$algo" == "sha256" ]] && prog="sha256sum"
    [[ "$algo" == "sha512" ]] && prog="sha512sum"
    [[ "$algo" == "crc32" ]] && { prog="md5sum"; command -v crc32 >/dev/null && prog="crc32"; }
    $prog "$file" | awk '{print $1}'
}

# ==============================================================================
# Main Orchestration
# ==============================================================================
init_environment() {
    ABS_PATH=$(realpath "$SEARCH_PATH")
    LOG_FILE="$ABS_PATH/duplicates.log"
    HASH_CSV="$ABS_PATH/duplicates.hashes.csv"
    IGNORE="$IGNORE,duplicates.log,duplicates.hashes.csv"
    > "$LOG_FILE"
    log_msg "Settings: Path=$ABS_PATH | Keep=$KEEP | Mode=$MODE | Algorithm=$ALGORITHM | Threads=$THREADS"
}

parse_args() {
    while [[ "$#" -gt 0 ]]; do
        case $1 in
            -p|--path) SEARCH_PATH="$2"; shift ;;
            -r|--recursive) RECURSIVE=true ;;
            -d|--dry-run) DRY_RUN=true ;;
            -k|--keep) KEEP="$2"; shift ;;
            -m|--mode) MODE="$2"; shift ;;
            -a|--algorithm) ALGORITHM="$2"; shift ;;
            -i|--ignore) IGNORE="$2"; shift ;;
            -t|--threads) THREADS="$2"; shift ;;
            -h|--help) usage ;;
            *) echo "Unknown parameter: $1"; usage ;;
        esac
        shift
    done
    [[ -z "$KEEP" ]] && { echo "Error: --keep is required."; usage; }
}

# Execution
parse_args "$@"
init_environment

read -r INITIAL_TOTAL INITIAL_FREE <<< "$(get_disk_space_raw)"
if [[ -n "$INITIAL_TOTAL" ]]; then
    log_msg "Free space before: $(format_disk_info "$INITIAL_TOTAL" "$INITIAL_FREE")" "Yellow"
else
    log_msg "Free space before: Unknown" "Yellow"
fi

log_msg "Scanning directory..."
FILES_STR=$(get_files | filter_hardlinks)
[[ -z "$FILES_STR" ]] && { log_msg "No files found."; exit 0; }

# mapfile needs newline separation, so we ensure filter_hardlinks outputs newlines
mapfile -t FILES_ARRAY <<< "$FILES_STR"
log_msg "Unique files found: ${#FILES_ARRAY[@]}"

declare -A HASH_CACHE
load_cache

declare -A DUPE_GROUPS
if [[ "$ALGORITHM" =~ md5|sha256|sha512|crc32 ]]; then
    log_msg "Pre-grouping by size..."
    # For speed, we do size/mtime pre-fetch
    POTENTIAL_TABLE=$(printf "%s\n" "${FILES_ARRAY[@]}" | xargs -d '\n' stat -c "%s %Y %n" 2>/dev/null | sort -n)
    SIZES_WITH_DUPS=$(echo "$POTENTIAL_TABLE" | awk '{print $1}' | uniq -d)
    [[ -z "$SIZES_WITH_DUPS" ]] && { log_msg "No duplicates found by size."; exit 0; }

    CANDIDATES=$(echo "$POTENTIAL_TABLE" | grep -Fwf <(echo "$SIZES_WITH_DUPS"))
    TOTAL_C=$(echo "$CANDIDATES" | wc -l)
    log_msg "Hashing $TOTAL_C candidates..."
    
    count=0
    while IFS= read -r line; do
        count=$((count + 1))
        SIZE=$(echo "$line" | awk '{print $1}')
        MTIME=$(echo "$line" | awk '{print $2}')
        FULL_PATH=$(echo "$line" | awk '{print substr($0, index($0,$3))}')
        REL_PATH="${FULL_PATH#$ABS_PATH/}"
        
        CACHE_KEY="$REL_PATH|$SIZE|$MTIME|$ALGORITHM"
        CACHE_VAL="${HASH_CACHE[$CACHE_KEY]}"
        
        if [[ -n "$CACHE_VAL" ]]; then
            HASH="$CACHE_VAL"
        else
            [[ "$SIZE" -gt 104857600 ]] && log_msg "  Large: $REL_PATH ($((SIZE/1024/1024)) MB)"
            HASH=$(get_single_hash "$ALGORITHM" "$FULL_PATH")
            save_hash_entry "$REL_PATH" "$SIZE" "$MTIME" "$ALGORITHM" "$HASH"
        fi
        [[ -n "$HASH" ]] && DUPE_GROUPS["$HASH"]+="$FULL_PATH|"
        (( count % 100 == 0 )) && printf "  Hashed %d/%d...\r" "$count" "$TOTAL_C" >&2
    done <<< "$CANDIDATES"
    echo "" >&2
else
    for file in "${FILES_ARRAY[@]}"; do
        NAME=$(basename "$file")
        DUPE_GROUPS["$NAME"]+="$file|"
    done
fi

log_msg "Processing groups..."
for hash in "${!DUPE_GROUPS[@]}"; do
    IFS='|' read -r -a group_files <<< "${DUPE_GROUPS[$hash]}"
    [[ ${#group_files[@]} -le 1 ]] && continue
    
    # Simple sort based on KEEP
    case "$KEEP" in
        highest) SORTED=($(for f in "${group_files[@]}"; do echo "${#f} $f"; done | sort -n | awk '{print $2}')) ;;
        deepest) SORTED=($(for f in "${group_files[@]}"; do echo "${#f} $f"; done | sort -nr | awk '{print $2}')) ;;
        *)
            SORTED=($(ls -t "${group_files[@]}" 2>/dev/null))
            [[ "$KEEP" == "oldest" ]] && SORTED=($(ls -tr "${group_files[@]}" 2>/dev/null))
            ;;
    esac

    KEEP_F="${SORTED[0]}"
    log_msg "KEEP: ${KEEP_F#$ABS_PATH/}" "Green"
    for dup in "${SORTED[@]:1}"; do
        [[ -z "$dup" ]] && continue
        if [[ "$DRY_RUN" == true ]]; then
            log_msg "  [DRY RUN] ${dup#$ABS_PATH/} -> $MODE" ; continue
        fi
        rm -f "$dup"
        case $MODE in
            delete)   log_msg "  Deleted ${dup#$ABS_PATH/}" "Red" ;;
            symlink)  ln -s "$KEEP_F" "$dup" ; log_msg "  Symlinked" "Cyan" ;;
            hardlink) ln "$KEEP_F" "$dup" ; log_msg "  Hardlinked" "Cyan" ;;
        esac
    done
done

read -r FINAL_TOTAL FINAL_FREE <<< "$(get_disk_space_raw)"
if [[ -n "$FINAL_FREE" ]]; then
    log_msg "Free space after: $(format_disk_info "$FINAL_TOTAL" "$FINAL_FREE")" "Yellow"
    if [[ -n "$INITIAL_FREE" ]]; then
        FREED=$(( FINAL_FREE - INITIAL_FREE ))
        FREED_GB_INT=$(( FREED / 1024 / 1024 ))
        # For decimal in bash, we can use a quick hack
        FREED_GB_DEC=$(( (FREED * 100 / 1024 / 1024) % 100 ))
        FREED_PERCENT=$(( 100 * FREED / INITIAL_TOTAL ))
        log_msg "Total space freed: $FREED_GB_INT.$FREED_GB_DEC GB ($FREED_PERCENT% of total disk space)" "Green"
    fi
else
    log_msg "Free space after: Unknown" "Yellow"
fi

log_msg "Done."
