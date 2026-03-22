export function responseTextToStatus(prefix, response, body) {
  const text = (body || "").replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
  return `${prefix} with HTTP ${response.status}${text ? `: ${text}` : ""}`;
}

export function previewBase64Payload(base64Value) {
  try {
    const text = atob(base64Value);
    return text.split("\n").slice(0, 12).join("\n");
  } catch {
    return "(non-text payload)";
  }
}

export async function fetchWithTimeout(url, options, timeoutMs) {
  const controller = new AbortController();
  const timeoutId = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    window.clearTimeout(timeoutId);
  }
}

export function legacyCopyText(value) {
  const textArea = document.createElement("textarea");
  textArea.value = value;
  textArea.setAttribute("readonly", "");
  textArea.style.position = "fixed";
  textArea.style.top = "-1000px";
  textArea.style.opacity = "0";
  document.body.appendChild(textArea);
  textArea.focus();
  textArea.select();
  textArea.setSelectionRange(0, textArea.value.length);
  let copied = false;
  try {
    copied = document.execCommand("copy");
  } finally {
    document.body.removeChild(textArea);
  }
  return copied;
}

export async function postText(url, body) {
  return fetch(url, {
    method: "POST",
    headers: { "Content-Type": "text/plain" },
    body,
  });
}
