# Analytics-Engine Developer Guide

## Sandbox flag

The sandbox modules (including `analytics-engine`) are excluded from the build by default. Enable them via:

```
systemProp.sandbox.enabled=true
```

Either in root `gradle.properties` (persistent) or as `-Dsandbox.enabled=true` per command. Without this, the module is invisible to Gradle.

## Toolchain

- JDK 25 (FFM is stable since JDK 22; sandbox modules override the project-wide JDK 21 target — see `sandbox/libs/dataformat-native/build.gradle`).
- Cargo (for the Rust dataformat-native library).

## Build / test

Compile only:
```
./gradlew -Dsandbox.enabled=true :sandbox:plugins:analytics-engine:compileJava
```

Single test class:
```
./gradlew -Dsandbox.enabled=true \
  :sandbox:plugins:analytics-engine:test \
  --tests "org.opensearch.analytics.planner.SortPlanShapeTests" \
  --rerun-tasks -Dtests.output=true
```

Full sandbox check (slow):
```
./gradlew -Dsandbox.enabled=true check -p sandbox
```

For ITs that need the Rust dylib, override with `OPENSEARCH_NATIVE_LIB=...` (see `sandbox/libs/dataformat-native/build.gradle`).
