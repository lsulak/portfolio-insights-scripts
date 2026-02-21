import re

class SECExtractor:
    # ==========================================
    # PRE-COMPILED REGEX PATTERNS 
    # Evaluated once upon module load for maximum throughput
    # ==========================================
    
    # 1. SGML Document Boundaries
    #   (PDFs, ZIPs, Graphics)
    PATTERN_DOCS = re.compile(
        r'<DOCUMENT>\s*<TYPE>(?:GRAPHIC|ZIP|EXCEL|PDF|JSON|XML|EX-10[14]\.[A-Z]{3}).*?</DOCUMENT>', 
        re.DOTALL | re.IGNORECASE
    )
    
    # 1.5A Binary Annihilators
    #   Mostly edge cases, and might be redundant in 99% of cases, as they will be caught by other patterns.
    PATTERN_UUENCODE = re.compile(
        r'begin\s+[0-7]{3}\s+[^\r\n]+\r?\n.*?\r?\nend[ \t]*\r?\n', 
        re.DOTALL | re.IGNORECASE
    )
    PATTERN_SGML_BIN = re.compile(
        r'<(PDF|GRAPHIC|ZIP|EXCEL)[^>]*>.*?</\1>', 
        re.DOTALL | re.IGNORECASE
    )
    PATTERN_BASE64_MASSIVE = re.compile(r'[a-zA-Z0-9+/=]{10000,}')
    
    # 1.5B The Geometric Entropy Shape 
    #   This is a universal entropy blob filter. Matches 10+ consecutive lines of 60+ characters that
    #   contain NO lowercase letters. It is mathematically impossible for this to be human SEC narrative.
    PATTERN_ENTROPY = re.compile(
        r'(?:[A-Z0-9!@#$%^&*()_\-+[\]{}\\/|<>?~`\'":;,. ]{60,}\r?\n){10,}'
    )
    
    # 2. Base64 Images
    PATTERN_IMAGES = re.compile(r'(data:image/[^;]+;base64,)[a-zA-Z0-9+/=]+')
    
    # 3. Structural Non-Narrative Blocks
    #   Destroys their contents also.
    PATTERN_STRUCT_BLOCKS = re.compile(
        r'<(script|style|head|title|meta|svg|ix:header|xbrli:context|xbrli:unit)[^>]*>.*?</\1>', 
        re.DOTALL | re.IGNORECASE
    )
    
    # 4. HTML Bloat Attributes
    #   Removes inline styles, classes, ids, and layout attributes from remaining tags
    PATTERN_STYLE_ATTR = re.compile(r'\s*style=[\'"][^\'"]*[\'"]', re.IGNORECASE)
    PATTERN_OTHER_ATTR = re.compile(
        r'\s*(?:class|id|cellpadding|cellspacing|valign|align|width|height|border|bgcolor)=[\'"][^\'"]*[\'"]', 
        re.IGNORECASE
    )
    PATTERN_INLINE_TAGS = re.compile(
        r'</?(div|span|font|a|b|i|u|strong|em|center)[^>]*>', 
        re.IGNORECASE
    )
    
    # 5. Whitespace Condensers
    #   SEC filings are notorious for hundreds of empty lines and massive indents
    #   Preserve empty table cells to maintain the 2D grid structure for the LLM
    PATTERN_EMPTY_CELLS = re.compile(r'<td>\s+</td>', re.IGNORECASE)
    PATTERN_EMPTY_ROWS = re.compile(r'<tr>\s*</tr>', re.IGNORECASE)
    PATTERN_SPACES = re.compile(r'[ \t]{2,}')
    PATTERN_NEWLINES = re.compile(r'\n{2,}')


    @classmethod
    def heuristic_sec_cleaner(cls, file_path: str, max_chars: int = 950000) -> str:
        """
        Strips binary bloat and converts the remaining HTML to LLM-native text,
        preserving financial tables and document hierarchy before truncation.
        """
        
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            raw_text = f.read()

        print(f"   🧹 [HEURISTIC CLEANER] Processing {file_path}...")

        # 1: Purge SEC Binary Exhibits
        print(f"      -> Stripping binary exhibits (length: {len(raw_text):,} chars)...")
        curr_text = cls.PATTERN_DOCS.sub('', raw_text)

        # 1.5: The Binary Annihilator
        print(f"      -> Stripping edge case binary blobs (length: {len(curr_text):,} chars)...")
        curr_text = cls.PATTERN_UUENCODE.sub('[UUENCODED BINARY REMOVED]\n', curr_text)
        curr_text = cls.PATTERN_SGML_BIN.sub(r'[\1 BLOCK REMOVED]', curr_text)
        curr_text = cls.PATTERN_BASE64_MASSIVE.sub('[MASSIVE BASE64 REMOVED]', curr_text)
        
        # MOVED: The Entropy Blob Filter must execute here, before whitespace is condensed
        print(f"      -> Vaporizing unlabeled gibberish blobs (length: {len(curr_text):,} chars)...")
        curr_text = cls.PATTERN_ENTROPY.sub('[UNLABELED GIBBERISH REMOVED]\n', curr_text)

        # 2. Purge inline base64 images
        print(f"      -> Scrubbing inline base64 image data (length: {len(curr_text):,} chars)...")
        curr_text = cls.PATTERN_IMAGES.sub(r'\1[REMOVED]', curr_text)

        # 3: Destroy useless massive blocks entirely
        print(f"      -> Removing non-narrative structural blocks (length: {len(curr_text):,} chars)...")
        curr_text = cls.PATTERN_STRUCT_BLOCKS.sub('', curr_text)

        # 4: Strip HTML Bloat Attributes
        print(f"      -> Removing HTML bloat attributes (length: {len(curr_text):,} chars)...")
        curr_text = cls.PATTERN_STYLE_ATTR.sub('', curr_text)
        curr_text = cls.PATTERN_OTHER_ATTR.sub('', curr_text)
        curr_text = cls.PATTERN_INLINE_TAGS.sub(' ', curr_text)

        # 5: Normalize & Condense Whitespace
        print(f"      -> Condensing whitespace (length: {len(curr_text):,} chars)...")
        curr_text = curr_text.replace("&#160;", " ").replace("&nbsp;", " ").replace("\xa0", " ")
        curr_text = cls.PATTERN_EMPTY_CELLS.sub('<td></td>', curr_text)
        curr_text = cls.PATTERN_EMPTY_ROWS.sub('', curr_text)
        curr_text = cls.PATTERN_SPACES.sub(' ', curr_text)
        curr_text = cls.PATTERN_NEWLINES.sub('\n', curr_text)

        # 6: The Hard Truncation Failsafe
        char_count = len(curr_text)
        if char_count > max_chars:
            print(f"      -> ✂️ Truncating from {char_count:,} down to {max_chars:,} chars.")
            curr_text = curr_text[:max_chars]
        else:
            print(f"      -> 🟢 Payload size safe: {char_count:,} chars.")

        # Save to a new file
        output_path = file_path.replace(".txt", "_cleaned.txt")
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(curr_text)

        return output_path
    