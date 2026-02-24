"""SEC Document Cleaner - Heuristic-based cleaning of raw SEC filings for LLM consumption."""

import logging
import os
import re

from helios.extraction_engine.sec.api import LocalEdgarDocument

logger = logging.getLogger(__name__)


class EdgarDocumentCleaner:
    """Strips binary bloat and HTML noise from raw Edgar (SEC) filings while preserving
    financial tables and narrative structure for downstream AI consumption."""

    def __init__(self, target_dir: str):
        self._target_dir = target_dir

    # ==========================================
    # PRE-COMPILED REGEX PATTERNS
    # Evaluated once upon module load for maximum throughput
    # ==========================================

    # 1. SGML Document Boundaries (PDFs, ZIPs, Graphics)
    PATTERN_DOCS = re.compile(
        r"<DOCUMENT>\s*<TYPE>(?:GRAPHIC|ZIP|EXCEL|PDF|JSON|XML|EX-10[14]\.[A-Z]{3}).*?</DOCUMENT>",
        re.DOTALL | re.IGNORECASE,
    )

    # 1.5A Binary Annihilators
    PATTERN_UUENCODE = re.compile(
        r"begin\s+[0-7]{3}\s+[^\r\n]+\r?\n.*?\r?\nend[ \t]*\r?\n",
        re.DOTALL | re.IGNORECASE,
    )
    PATTERN_SGML_BIN = re.compile(
        r"<(PDF|GRAPHIC|ZIP|EXCEL)[^>]*>.*?</\1>",
        re.DOTALL | re.IGNORECASE,
    )
    PATTERN_BASE64_MASSIVE = re.compile(r"[a-zA-Z0-9+/=]{10000,}")

    # 1.5B The Geometric Entropy Filter
    #   Matches 10+ consecutive lines of 60+ characters with NO lowercase letters.
    #   It is mathematically impossible for this to be human SEC narrative.
    PATTERN_ENTROPY = re.compile(r'(?:[A-Z0-9!@#$%^&*()_\-+[\]{}\\/|<>?~`\'":;,. ]{60,}\r?\n){10,}')

    # 2. Base64 Images
    PATTERN_IMAGES = re.compile(r"(data:image/[^;]+;base64,)[a-zA-Z0-9+/=]+")

    # 3. Structural Non-Narrative Blocks (destroys their contents)
    PATTERN_STRUCT_BLOCKS = re.compile(
        r"<(script|style|head|title|meta|svg|ix:header|xbrli:context|xbrli:unit)[^>]*>.*?</\1>",
        re.DOTALL | re.IGNORECASE,
    )

    # 4. HTML Bloat Attributes
    PATTERN_STYLE_ATTR = re.compile(r'\s*style=[\'"][^\'"]*[\'"]', re.IGNORECASE)
    PATTERN_OTHER_ATTR = re.compile(
        r'\s*(?:class|id|cellpadding|cellspacing|valign|align|width|height|border|bgcolor)=[\'"][^\'"]*[\'"]',
        re.IGNORECASE,
    )
    PATTERN_INLINE_TAGS = re.compile(
        r"</?(div|span|font|a|b|i|u|strong|em|center)[^>]*>",
        re.IGNORECASE,
    )

    # 5. Whitespace Condensers
    #   Preserve empty table cells to maintain 2D grid structure for the LLM
    PATTERN_EMPTY_CELLS = re.compile(r"<td>\s+</td>", re.IGNORECASE)
    PATTERN_EMPTY_ROWS = re.compile(r"<tr>\s*</tr>", re.IGNORECASE)
    PATTERN_SPACES = re.compile(r"[ \t]{2,}")
    PATTERN_NEWLINES = re.compile(r"\n{2,}")

    # ==========================================
    # PUBLIC API
    # ==========================================

    def clean_and_minify(self, document: LocalEdgarDocument, max_chars: int = 950_000) -> str:
        """Clean a raw Edgar (SEC) filing for AI consumption.

        Strips binary bloat and converts remaining HTML to LLM-native text,
        preserving financial tables and document hierarchy before truncation.

        Args:
            document: The EDGAR document to clean.
            max_chars: Maximum character limit after cleaning.

        Returns:
            Path to the cleaned output file.
        """
        if not os.path.exists(document.file_path_raw):
            raise FileNotFoundError(f"Source file not found: {document.file_path_raw}")

        with open(document.file_path_raw, "r", encoding="utf-8", errors="ignore") as f:
            raw_text = f.read()

        logger.info(f"🧹 Cleaning {os.path.basename(document.file_path_raw)}...")

        text = self._remove_binary_exhibits(raw_text)
        text = self._remove_inline_images(text)
        text = self._remove_structural_blocks(text)
        text = self._remove_html_bloat(text)
        text = self._condense_whitespace(text)
        text = self._truncate_if_needed(text, max_chars)

        return self._save_cleaned_file(document, text)

    # ==========================================
    # PRIVATE UTILS AND CLEANING STEPS
    # ==========================================

    def _log_step(self, message: str, char_count: int) -> None:
        """Helper to log cleaning steps with character count."""
        logger.debug(f"   -> {message} (length: {char_count:,} chars)")

    def _remove_binary_exhibits(self, text: str) -> str:
        """Remove SGML document boundaries and binary blobs."""
        self._log_step("Stripping binary exhibits", len(text))
        text = self.PATTERN_DOCS.sub("", text)

        self._log_step("Stripping edge case binary blobs", len(text))
        text = self.PATTERN_UUENCODE.sub("[UUENCODED BINARY REMOVED]\n", text)
        text = self.PATTERN_SGML_BIN.sub(r"[\1 BLOCK REMOVED]", text)
        text = self.PATTERN_BASE64_MASSIVE.sub("[MASSIVE BASE64 REMOVED]", text)

        # Entropy filter must run before whitespace condensation
        self._log_step("Vaporizing unlabeled gibberish blobs", len(text))
        text = self.PATTERN_ENTROPY.sub("[UNLABELED GIBBERISH REMOVED]\n", text)

        return text

    def _remove_inline_images(self, text: str) -> str:
        """Remove inline base64 encoded images."""
        self._log_step("Scrubbing inline base64 image data", len(text))
        return self.PATTERN_IMAGES.sub(r"\1[REMOVED]", text)

    def _remove_structural_blocks(self, text: str) -> str:
        """Remove non-narrative structural blocks."""
        self._log_step("Removing non-narrative structural blocks", len(text))
        return self.PATTERN_STRUCT_BLOCKS.sub("", text)

    def _remove_html_bloat(self, text: str) -> str:
        """Strip HTML bloat attributes and inline tags."""
        self._log_step("Removing HTML bloat attributes", len(text))
        text = self.PATTERN_STYLE_ATTR.sub("", text)
        text = self.PATTERN_OTHER_ATTR.sub("", text)
        text = self.PATTERN_INLINE_TAGS.sub(" ", text)
        return text

    def _condense_whitespace(self, text: str) -> str:
        """Normalize and condense whitespace while preserving structure."""
        self._log_step("Condensing whitespace", len(text))
        text = text.replace("&#160;", " ").replace("&nbsp;", " ").replace("\xa0", " ")
        text = self.PATTERN_EMPTY_CELLS.sub("<td></td>", text)
        text = self.PATTERN_EMPTY_ROWS.sub("", text)
        text = self.PATTERN_SPACES.sub(" ", text)
        text = self.PATTERN_NEWLINES.sub("\n\n", text)  # Preserve paragraph breaks
        return text.strip()

    def _truncate_if_needed(self, text: str, max_chars: int) -> str:
        """Truncate text if it exceeds max_chars limit."""
        char_count = len(text)
        if char_count > max_chars:
            logger.warning(f"   ✂️ Truncating from {char_count:,} to {max_chars:,} chars")
            return text[:max_chars]
        else:
            logger.info(f"   🟢 Payload size safe: {char_count:,} chars")
            return text

    def _save_cleaned_file(self, document: LocalEdgarDocument, content: str) -> str:
        """Save cleaned content to target directory."""
        output_path = os.path.join(
            self._target_dir,
            document.form_type,
            f"{document.submission_year}_{document.submission_order_for_the_year:06d}.html",
        )

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(content)

        logger.info(f"   💾 Saved cleaned file to: {output_path}")
        return output_path
