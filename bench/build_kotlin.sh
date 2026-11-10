#!/usr/bin/env bash
# Builds the Kotlin lib + app (incl. Bench.kt) with the Kotlin compiler directly, reusing the jars
# Gradle already resolved into ~/.gradle. Used on low-memory machines where Gradle itself gets OOM-killed;
# on a normal machine `./gradlew :app:installDist` is equivalent.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
C=~/.gradle/caches/modules-2/files-2.1
jar() { find "$C/$1" -name "$2" | head -1; }
mvn() { # group/artifact version -> downloaded jar path (for compiler deps Gradle didn't keep)
  local f="$ROOT/bench/.jars/$(basename "$1")-$2.jar"
  [ -f "$f" ] || { mkdir -p "$ROOT/bench/.jars"; curl -sSfL -o "$f" "https://repo1.maven.org/maven2/$1/$2/$(basename "$1")-$2.jar"; }
  echo "$f"
}
KV=1.8.20
COMPILER_CP="$(jar org.jetbrains.kotlin/kotlin-compiler-embeddable/$KV 'kotlin-compiler-embeddable-*.jar'):$(jar org.jetbrains.kotlin/kotlin-stdlib/$KV 'kotlin-stdlib-*.jar'):$(jar org.jetbrains.kotlin/kotlin-daemon-embeddable/$KV '*.jar'):$(jar org.jetbrains.intellij.deps/trove4j '*.jar'):$(mvn org/jetbrains/kotlin/kotlin-script-runtime $KV):$(mvn org/jetbrains/kotlin/kotlin-reflect 1.6.10):$(jar org.jetbrains/annotations '*.jar')"
RUNTIME_CP="$(jar org.jetbrains.kotlin/kotlin-stdlib/$KV 'kotlin-stdlib-*.jar'):$(jar org.jetbrains.kotlin/kotlin-stdlib-jdk8/$KV '*.jar'):$(jar org.jetbrains.kotlin/kotlin-stdlib-jdk7/$KV '*.jar'):$(jar org.jetbrains.kotlinx/kotlinx-coroutines-core-jvm/1.7.1 '*.jar'):$(jar io.methvin/directory-watcher '*.jar'):$(jar net.java.dev.jna/jna/5.12.1 '*.jar'):$(jar org.slf4j/slf4j-api '*.jar'):$(jar com.google.guava/guava '*.jar'):$(jar com.google.guava/failureaccess '*.jar'):$(jar it.unimi.dsi/fastutil '*.jar')"
OUT="$ROOT/bench/.kotlin-out"
rm -rf "$OUT" && mkdir -p "$OUT"
java -Xmx450m -XX:+UseSerialGC -cp "$COMPILER_CP" org.jetbrains.kotlin.cli.jvm.K2JVMCompiler \
  -no-stdlib -no-reflect -jvm-target 1.8 -cp "$RUNTIME_CP" -d "$OUT" \
  $(find "$ROOT/lib/src/main/kotlin" "$ROOT/app/src/main/kotlin" -name '*.kt')
echo "$OUT:$RUNTIME_CP" > "$ROOT/bench/.kotlin-classpath"
echo "built -> $OUT"
