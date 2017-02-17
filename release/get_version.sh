#!/usr/bin/env bash

MVN_VERSION=""
VERSION_FILE=$( mktemp mvn_project_version_XXXXX )
trap "rm -f -- \"$VERSION_FILE\"" INT EXIT

mvn -Dexec.executable="echo" \
    -Dexec.args='${project.version}' \
    -Dexec.outputFile="$VERSION_FILE" \
    --non-recursive \
    --batch-mode \
    org.codehaus.mojo:exec-maven-plugin:1.3.1:exec > /dev/null 2>&1 ||
    { echo "Maven invocation failed!" 1>&2; exit 1; }

# if you just care about the first line of the version, which will be
# sufficent for pretty much every use case I can imagine, you can use
# the read builtin
[ -s "$VERSION_FILE" ] && read -r MVN_VERSION < "$VERSION_FILE"

# Otherwise, you could use cat.
# Note that this still has issues when there are leading whitespaces
# in the multiline version string
#MVN_VERSION=$( cat "$VERSION_FILE" )

echo "$MVN_VERSION"