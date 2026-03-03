# Shell Benchmark Report

| Metric | Tauri | Electron | Delta (Electron vs Tauri) |
| --- | ---: | ---: | ---: |
| Startup (ms) | 680.0 | 1240.0 | +82.4% |
| Idle memory (MB) | 164.0 | 318.0 | +93.9% |
| Render FPS | 58.0 | 61.0 | +5.2% |

## Interpretation

- Positive startup/memory delta means Electron is heavier than Tauri.
- Positive FPS delta means Electron renders faster in the sampled scenario.
- Keep Tauri as default unless Electron shows a requirement-level advantage.
