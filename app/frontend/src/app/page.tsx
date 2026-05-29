import { redirect } from "next/navigation";

// The default landing page goes straight to Ask, as the IA spec requires.
export default function RootIndex() {
  redirect("/ask");
}
