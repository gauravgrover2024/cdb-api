export default function withCors(handler) {
  return async function (req, res) {
    // CORS headers FIRST – always
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader(
      "Access-Control-Allow-Methods",
      "GET,POST,PUT,DELETE,OPTIONS",
    );
    res.setHeader(
      "Access-Control-Allow-Headers",
      "Content-Type, Authorization",
    );

    // Handle preflight
    if (req.method === "OPTIONS") {
      return res.status(200).end();
    }

    // Then run actual handler
    return handler(req, res);
  };
}
