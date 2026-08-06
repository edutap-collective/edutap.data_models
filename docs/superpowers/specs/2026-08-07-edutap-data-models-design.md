# `edutap.data_models` — Design

**Datum:** 2026-08-07
**Status:** Entwurf, Repository noch nicht angelegt

Ein Paket für die Verträge, die **mehrere** eduTAP-Pakete teilen: Vokabulare, Nachrichten-Hülle und wiederverwendbare Settings-Bausteine.

## Warum

Drei Sorten Duplikat, alle belegt.

**Vokabulare in drei divergierenden Kopien.** `PassLifecycleState` existiert in `lmu_edutap_common` (acht Werte, drei Zustandsautomaten vermischt), in `lmu_edutap_full_view` und in `edutap.data_provider` (je sechs). `WalletType` ebenso, in vier Schreibweisen. Die Docstring von `edutap.data_provider.vocabulary` hält bereits fest, dass ihre Schreibweisen die älteren ablösen und das Angleichen Folgearbeit ist.

**Der Nachrichtenvertrag.** Header-Block, Key-Regel und DLQ-Hülle aus dem Kafka-Schema müssen in sechs Paketen identisch sein. Sechs Kopien divergieren garantiert.

**Settings, die überall gleich heißen sollen.** `sentry_dsn` ist das klarste Beispiel: In jedem Container derselbe Feldname, vom Swarm-Compose-File unterschiedlich besetzt. Heute stehen dafür zwei Muster nebeneinander — `LMU_EDUTAP_SENTRY_DSN_FILE` als Docker-Secret beim Backend, `GOOGLE_CALLBACK_SENTRY_DSN` als Klartext im Prod-Overlay.

## Was hineingehört

| Modul | Inhalt |
|---|---|
| `vocabulary` | `PassLifecycleState`, `WalletType`, `FieldKind`, `Provider` — die kontrollierten Werte |
| `messaging` | Header-Namen und -Bau, Key-Regel, DLQ-Hülle, logische Topic-Namen |
| `settings` | wiederverwendbare pydantic-settings-Bausteine: `SentrySettings`, `KafkaSettings` (Bootstrap + Topic-Präfix + Consumer-Group), `ObservabilitySettings` |
| `contracts` | die Nutzlast-Verträge, zuerst `pass-state/v1` |

### Die Settings-Bausteine

Als Mixins, nicht als fertige Settings-Klasse — jedes Paket erbt, was es braucht, und behält seinen eigenen `env_prefix`:

```python
class KafkaSettings(BaseSettings):
    bootstrap_servers: str
    topic_prefix: str                    # kein Default -- Start bricht ab
    consumer_group: str | None = None

    def topic(self, name: str) -> str:
        return f"{self.topic_prefix}.{name}"
```

Dass `topic_prefix` keinen Default hat, ist Absicht: Ein fehlender Wert soll den Start abbrechen, statt still in die Topics der anderen Umgebung zu schreiben.

## Was nicht hineingehört

**Tabellendefinitionen einzelner Pakete.** `edutap.db_definitions` beruht darauf, dass jedes Paket sein Schema mitbringt und per Entry-Point anmeldet — daraus folgt die Eigentümerschaft und damit der Kollisionscheck. Zöge man Tabellen in ein gemeinsames Paket, wäre für kein Schema mehr jemand zuständig.

```{note}
Die Tabellen des `edutap.data_provider` sind ein möglicher Sonderfall: `person_view` und `pass_state` sind **Vertrag** für externe Konsumenten, nicht Implementierungsdetail eines Dienstes. Ob sie deshalb hierher gehören, ist offen und in der Datenbank-Referenz zu entscheiden — zusammen mit der Frage nach Schema oder Präfix.
```

**Alles LMU-Spezifische.** `lmu_edutap_common` bleibt, was es ist; die `edutap.*`-Pakete dürfen nicht davon abhängen.

## Abhängigkeitsrichtung

`edutap.data_models` hängt von **nichts** aus dem eduTAP-Bestand ab — nur von `pydantic` und `pydantic-settings`. Alles andere darf davon abhängen. Eine Bibliothek, die selbst Dienste kennt, ist keine.

Dass Pakete in `edutap-collective` dadurch untereinander gekoppelt werden, ist der bewusst gezahlte Preis. Wer nur den Google-Callback-Handler nutzt, zieht sie mit — bei dieser Größe vertretbar, gemessen an sechs auseinanderlaufenden Kopien des Header-Vertrags.

## Offene Punkte

* **Repository anlegen** — `edutap-collective`, öffentlich. Nach der Namensregel wäre auch `edutap.data_models` korrekt: kein Wallet-Paket, also kein `wallet_`-Segment.
* **Sentry oder Bugsink** — oder beides während einer Übergangszeit. Bei zwei Zielen braucht es zwei Felder, nicht eines.
* **Migrationsreihenfolge** — welche Pakete zuerst umgestellt werden. Vorschlag: `edutap.data_provider` als Erstes, weil dessen Vokabular ohnehin als das führende gilt.
* **Verhältnis zu `edutap.db_definitions`** — beide sind paketübergreifend. Klar trennen: `db_definitions` ist ein **Werkzeug** ohne Laufzeitrolle, `data_models` eine **Laufzeit-Bibliothek**.

## Hausstil

Wie `edutap.data_provider`: Makefile, tox über die unterstützten Python-Versionen, ruff, ty, Renovate als gehostete App, **kein** `uv.lock` (Bibliothek), Specs unter `docs/superpowers/`.
