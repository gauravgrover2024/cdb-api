export const formatAciInlineVariantName = (value = "") =>
  String(value || "")
    .trim()
    .split(/\s+/)
    .map((word) => {
      if (/^ivt$/i.test(word)) return "iVT";
      if (/^(dct|amt|at|mt|cvt)$/i.test(word)) return word.toUpperCase();
      if (/^sx$/i.test(word)) return "SX";
      if (/^htx$/i.test(word)) return "HTX";
      if (/^abs$/i.test(word)) return "ABS";
      if (/^[A-Z0-9()]+$/.test(word)) return word;
      return word.charAt(0).toUpperCase() + word.slice(1);
    })
    .join(" ");
