import { run } from "./runtime.ts";
import { serve } from "./stdio.ts";

// No default filesystem/shell tools, global extensions, or alternative runtime.
serve(run);
