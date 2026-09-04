#!/bin/sh
set -eu

prefix=/usr/libexec/rebecca-accel
arch=${ARCH:-x86_64}
loader=ld-musl-${arch}.so.1
root=/tmp/rebecca-accel-root
dest=$root$prefix
modules=$dest/lib/accel-ppp

apk add --no-cache accel-ppp openssl >/dev/null
test -f /usr/lib/accel-ppp/libsstp.so
rm -rf "$root"
mkdir -p "$dest/sbin" "$dest/bin" "$dest/lib" "$modules" "$dest/lib/ossl-modules" "$dest/share/accel-ppp"
cp /usr/sbin/accel-pppd "$dest/sbin/accel-pppd.bin"
cp /usr/bin/accel-cmd "$dest/bin/accel-cmd.bin"
cp /usr/lib/accel-ppp/*.so "$modules/"
cp -a /usr/share/accel-ppp/. "$dest/share/accel-ppp/"
cp "/lib/$loader" "$dest/lib/$loader"
test ! -f /usr/lib/ossl-modules/legacy.so || cp /usr/lib/ossl-modules/legacy.so "$dest/lib/ossl-modules/legacy.so"

copy_deps() {
	for file in "$@"; do
		test -f "$file" || continue
		ldd "$file" 2>/dev/null | awk '/=>/ {print $3}' | while read -r library; do
			test -f "$library" || continue
			test -e "$dest/lib/$(basename "$library")" || cp -L "$library" "$dest/lib/"
		done
	done
}
copy_deps "$dest/sbin/accel-pppd.bin" "$dest/bin/accel-cmd.bin" "$modules"/*.so "$dest/lib/ossl-modules/legacy.so"
copy_deps "$dest"/lib/*.so*

printf '%s\n' '#!/bin/sh' "base=$prefix" "export OPENSSL_MODULES=\${OPENSSL_MODULES:-\$base/lib/ossl-modules}" "exec \"\$base/lib/$loader\" --library-path \"\$base/lib:\$base/lib/accel-ppp\" \"\$base/sbin/accel-pppd.bin\" \"\$@\"" > "$dest/sbin/accel-pppd"
printf '%s\n' '#!/bin/sh' "base=$prefix" "exec \"\$base/lib/$loader\" --library-path \"\$base/lib:\$base/lib/accel-ppp\" \"\$base/bin/accel-cmd.bin\" \"\$@\"" > "$dest/bin/accel-cmd"
chmod 0755 "$dest/sbin/accel-pppd" "$dest/bin/accel-cmd"

mkdir -p /out
tar czf /out/rebecca-accel-ppp-linux-${ASSET_ARCH}.tar.gz -C "$root" usr
