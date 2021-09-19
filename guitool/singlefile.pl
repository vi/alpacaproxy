#!/usr/bin/perl -w

use strict;
use warnings;
use utf8;
use open qw(:std :utf8);

my $wasmfile;
my $jsfile;
my $cssfile;

open I, "<", "dist/index.html";

while(<I>) {
    if (m!\<link.*href=\"([^"]+)\"!) {
        my $f = $1;
        $wasmfile="dist/$f" if $f =~ m!\.wasm$!;
        $jsfile  ="dist/$f" if $f =~ m!\.js$!;
        $cssfile ="dist/$f" if $f =~ m!\.css$!;
    }
}
close I;

print "wasm module: $wasmfile\n";
print "js file: $jsfile\n";
print "css file: $cssfile\n";


system "base64 -w0 < $wasmfile > wasmdata.txt";

open F, "<", $jsfile;
my $jscontent = do { local $/; <F> };
close F;

open F, "<", $cssfile;
my $csscontent = do { local $/; <F> };
close F;

open F, "<", "wasmdata.txt";
my $wasmcontent = do { local $/; <F> };
close F;

# make tidy put tags on their own lines.
# Yes, it _is_ parsing HTML with regexes.
system "tidy -q dist/index.html > tidy.html";

open O, ">", "output.html";
open I, "<", "tidy.html";
while(<I>) {
    if (m!\<link.*href=\"([^"]+)\"!) {
        next;
    }
    if (m!application/wasm"!) {
        # tidy decided to put that on its own line
        next;
    }
    if (m!</head>!) {
        print O "<style>\n";
        print O $csscontent;
        print O "</style>\n";
    }
    if (m!\<script!) {
        print O '<script type="module">'."\n";
        print O $jscontent;
        print O 'var wasm_base64 = "';
        print O $wasmcontent;
        print O '";'."\n";
        print O 'let wasm_buffer = Uint8Array.from(atob(wasm_base64), c => c.charCodeAt(0)).buffer;'."\n";
        print O 'wasm_base64 = "";'."\n";
        print O 'init(wasm_buffer);'."\n";
        print O '</script></body></html>'."\n";
        last;
    }
    print O $_;
}
close I;
close O;
print "Written output.html\n";
