playout takes rustls-webpki 0.103.16 and webpki-roots / webpki-root-certs 1.0.10, which reject certificate paths through the distrusted emSign intermediates (RUSTSEC-2026-0381, -0382, -0383).
